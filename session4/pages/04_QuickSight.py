
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
    page_title="Amazon QuickSight Analytics Platform",
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
    'purple': '#7B68EE'
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
            border-left: 5px solid {AWS_COLORS['light_blue']};
        }}
        
        .spice-card {{
            background: linear-gradient(135deg, {AWS_COLORS['purple']} 0%, {AWS_COLORS['primary']} 100%);
            padding: 20px;
            border-radius: 15px;
            color: white;
            margin: 15px 0;
        }}
    </style>
    """, unsafe_allow_html=True)

def initialize_session_state():
    """Initialize session state variables"""
    common.initialize_session_state()
    if 'session_started' not in st.session_state:
        st.session_state.session_started = True
        st.session_state.dashboards_created = []
        st.session_state.data_sources_connected = []
        st.session_state.spice_usage = 0

def create_sidebar():
    """Create sidebar with app information and controls"""
    with st.sidebar:
        common.render_sidebar()
        
        # About section (collapsed by default)
        with st.expander("📖 About this App", expanded=False):
            st.markdown("""
            **Topics Covered:**
            - 📊 QuickSight - Scalable serverless business intelligence service
            - 💾 Accessing data with QuickSight - SPICE engine and data connectivity
            - 🔗 QuickSight embedded analytics - Integration with applications
            
            **Learning Objectives:**
            - Master QuickSight's BI capabilities and features
            - Understand SPICE engine for fast data processing
            - Learn embedding techniques for applications
            - Explore data visualization and dashboard creation
            """)

def create_quicksight_architecture():
    """Create mermaid diagram for QuickSight architecture"""
    return """
    graph TB
        subgraph "Data Sources"
            S3[Amazon S3<br/>📁 Data Lakes]
            RDS[Amazon RDS<br/>🗄️ Relational Data]
            REDSHIFT[Amazon Redshift<br/>🏭 Data Warehouse]
            ATHENA[Amazon Athena<br/>🔍 Query Service]
            FILES[Files<br/>📊 CSV, Excel, JSON]
        end
        
        subgraph "QuickSight Platform"
            QS[Amazon QuickSight<br/>📊 BI Platform]
            SPICE[SPICE Engine<br/>⚡ In-Memory Processing]
            ML[ML Insights<br/>🤖 Anomaly Detection]
        end
        
        subgraph "Output & Access"
            DASHBOARDS[Interactive Dashboards<br/>📈 Visualizations]
            EMBED[Embedded Analytics<br/>🔗 Applications]
            REPORTS[Reports<br/>📋 Scheduled]
            Q[QuickSight Q<br/>💬 Natural Language]
        end
        
        S3 --> QS
        RDS --> QS
        REDSHIFT --> QS
        ATHENA --> QS
        FILES --> QS
        
        QS --> SPICE
        QS --> ML
        
        SPICE --> DASHBOARDS
        QS --> EMBED
        QS --> REPORTS
        QS --> Q
        
        style QS fill:#FF9900,stroke:#232F3E,color:#fff
        style SPICE fill:#4B9EDB,stroke:#232F3E,color:#fff
        style ML fill:#3FB34F,stroke:#232F3E,color:#fff
        style DASHBOARDS fill:#7B68EE,stroke:#232F3E,color:#fff
    """

def create_spice_architecture():
    """Create mermaid diagram for SPICE architecture"""
    return """
    graph LR
        subgraph "Data Sources"
            DB1[(Database)]
            S3[Amazon S3]
            API[REST APIs]
            FILES[Files]
        end
        
        subgraph "SPICE Engine"
            IMPORT[Data Import<br/>🔄 ETL Process]
            MEMORY[In-Memory Storage<br/>💾 Columnar Format]
            CACHE[Query Cache<br/>⚡ Fast Retrieval]
            COMPRESS[Compression<br/>🗜️ Space Efficient]
        end
        
        subgraph "QuickSight Analytics"
            ANALYSIS[Analysis<br/>📊 Interactive]
            DASH[Dashboard<br/>📈 Visualizations]
            CALC[Calculations<br/>🧮 Computed Fields]
        end
        
        DB1 --> IMPORT
        S3 --> IMPORT
        API --> IMPORT
        FILES --> IMPORT
        
        IMPORT --> MEMORY
        MEMORY --> CACHE
        MEMORY --> COMPRESS
        
        CACHE --> ANALYSIS
        CACHE --> DASH
        CACHE --> CALC
        
        style IMPORT fill:#FF9900,stroke:#232F3E,color:#fff
        style MEMORY fill:#4B9EDB,stroke:#232F3E,color:#fff
        style CACHE fill:#3FB34F,stroke:#232F3E,color:#fff
        style COMPRESS fill:#7B68EE,stroke:#232F3E,color:#fff
    """

def create_embedded_analytics_flow():
    """Create mermaid diagram for embedded analytics"""
    return """
    graph TB
        subgraph "Development Process"
            CREATE[Create Dashboard<br/>📊 QuickSight Console]
            PERMISSIONS[Apply Permissions<br/>🔐 IAM Roles]
            AUTH[Authenticate App Server<br/>🔑 AWS Credentials]
            EMBED[Embed via JavaScript SDK<br/>💻 Web/Mobile]
        end
        
        subgraph "Application Architecture"
            APP[Your Application<br/>🌐 Web/Mobile]
            SERVER[App Server<br/>⚙️ Authentication]
            QUICKSIGHT[QuickSight Service<br/>📊 AWS Cloud]
            USERS[End Users<br/>👥 Dashboard Viewers]
        end
        
        subgraph "Authentication Flow"
            IDP[Identity Provider<br/>🏢 Active Directory]
            COGNITO[Amazon Cognito<br/>👤 User Pool]
            SAML[SAML SSO<br/>🔐 Federation]
        end
        
        CREATE --> PERMISSIONS
        PERMISSIONS --> AUTH
        AUTH --> EMBED
        
        USERS --> APP
        APP --> SERVER
        SERVER --> QUICKSIGHT
        
        IDP --> SERVER
        COGNITO --> SERVER
        SAML --> SERVER
        
        style CREATE fill:#FF9900,stroke:#232F3E,color:#fff
        style AUTH fill:#4B9EDB,stroke:#232F3E,color:#fff
        style APP fill:#3FB34F,stroke:#232F3E,color:#fff
        style QUICKSIGHT fill:#7B68EE,stroke:#232F3E,color:#fff
    """

def generate_sample_data():
    """Generate sample sales data for demonstrations"""
    np.random.seed(42)
    
    # Generate sales data
    dates = pd.date_range(start='2024-01-01', end='2024-12-31', freq='D')
    products = ['Product A', 'Product B', 'Product C', 'Product D', 'Product E']
    regions = ['North America', 'Europe', 'Asia-Pacific', 'Latin America']
    
    data = []
    for date in dates:
        for product in products:
            for region in regions:
                sales = np.random.normal(1000, 200)
                quantity = np.random.poisson(50)
                data.append({
                    'date': date,
                    'product': product,
                    'region': region,
                    'sales_amount': max(sales, 0),
                    'quantity': quantity,
                    'profit_margin': np.random.uniform(0.1, 0.4)
                })
    
    return pd.DataFrame(data)

def quicksight_tab():
    """Content for Amazon QuickSight tab"""
    st.markdown("## 📊 Amazon QuickSight")
    st.markdown("*Scalable, serverless business intelligence service with machine learning insights*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 What is Amazon QuickSight?
    Amazon QuickSight is a cloud-scale business intelligence service that enables:
    - **Serverless Architecture**: Automatically scales from tens to tens of thousands of users
    - **Machine Learning Insights**: Built-in anomaly detection and forecasting
    - **Embedded Analytics**: Integrate dashboards into your applications
    - **Natural Language Queries**: Ask questions in plain English with QuickSight Q
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Architecture diagram
    st.markdown("#### 🏗️ QuickSight Platform Architecture")
    common.mermaid(create_quicksight_architecture(), height=650)
    
    # Key features showcase
    st.markdown("#### ⭐ Key Features & Capabilities")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📈 Interactive Dashboards
        - **Drag-and-Drop Interface**
        - **15+ Visualization Types**
        - **Real-time Data Updates**
        - **Mobile Responsive**
        - **Collaborative Sharing**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🤖 Machine Learning Insights
        - **Anomaly Detection**
        - **Forecasting**
        - **Auto-narratives**
        - **Key Drivers Analysis**
        - **What-if Analysis**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔐 Enterprise Security
        - **Row-level Security**
        - **Active Directory Integration**
        - **VPC Connectivity**
        - **Encryption at Rest/Transit**
        - **Audit Trails**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive dashboard simulator
    st.markdown("#### 🎨 Dashboard Builder Simulator")
    
    # Generate sample data
    df = generate_sample_data()
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.markdown("##### Configuration Panel")
        chart_type = st.selectbox("Visualization Type", [
            "Line Chart", "Bar Chart", "Pie Chart", "Scatter Plot", 
            "Heat Map", "Gauge", "Funnel", "Waterfall"
        ])
        
        metric = st.selectbox("Metric", ["sales_amount", "quantity", "profit_margin"])
        dimension = st.selectbox("Dimension", ["product", "region", "date"])
        
        # Time filter
        time_filter = st.select_slider(
            "Time Period",
            options=["Last 7 Days", "Last 30 Days", "Last 90 Days", "Year to Date", "All Time"],
            value="Last 90 Days"
        )
    
    with col2:
        st.markdown("##### Live Preview")
        
        # Filter data based on selection
        if time_filter == "Last 7 Days":
            filtered_df = df[df['date'] >= df['date'].max() - pd.Timedelta(days=7)]
        elif time_filter == "Last 30 Days":
            filtered_df = df[df['date'] >= df['date'].max() - pd.Timedelta(days=30)]
        elif time_filter == "Last 90 Days":
            filtered_df = df[df['date'] >= df['date'].max() - pd.Timedelta(days=90)]
        else:
            filtered_df = df
        
        # Create visualization based on selection
        if chart_type == "Line Chart" and dimension == "date":
            daily_data = filtered_df.groupby('date')[metric].sum().reset_index()
            fig = px.line(daily_data, x='date', y=metric, title=f'{metric.title()} Over Time')
        elif chart_type == "Bar Chart":
            grouped_data = filtered_df.groupby(dimension)[metric].sum().reset_index()
            fig = px.bar(grouped_data, x=dimension, y=metric, title=f'{metric.title()} by {dimension.title()}')
        elif chart_type == "Pie Chart":
            grouped_data = filtered_df.groupby(dimension)[metric].sum().reset_index()
            fig = px.pie(grouped_data, names=dimension, values=metric, title=f'{metric.title()} Distribution')
        else:
            # Default to bar chart
            grouped_data = filtered_df.groupby(dimension)[metric].sum().reset_index()
            fig = px.bar(grouped_data, x=dimension, y=metric, title=f'{metric.title()} by {dimension.title()}')
        
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Analytics insights simulation
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    insights = generate_insights(filtered_df, metric, dimension)
    st.markdown(f"""
    ### 🔍 ML-Powered Insights
    **Key Finding**: {insights['key_finding']}  
    **Trend**: {insights['trend']}  
    **Anomaly Detected**: {insights['anomaly']}  
    **Recommendation**: {insights['recommendation']}
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Pricing and editions
    st.markdown("#### 💰 QuickSight Editions & Pricing")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown("""
        ### Standard Edition
        **$9 per user/month**
        
        ✅ Core BI Features  
        ✅ Data Sources Connectivity  
        ✅ Sharing & Collaboration  
        ✅ Mobile Access  
        ✅ Email Reports  
        
        **Best for:** Small to medium teams
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown("""
        ### Enterprise Edition
        **$18 per user/month**
        
        ✅ Everything in Standard  
        ✅ ML Insights & Forecasting  
        ✅ Embedded Analytics  
        ✅ Row-level Security  
        ✅ Active Directory Integration  
        
        **Best for:** Enterprise organizations
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 QuickSight SDK Examples")
    
    tab1, tab2, tab3 = st.tabs(["Create Dashboard", "Manage Users", "Data Sources"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
import boto3
import json
from datetime import datetime

# Initialize QuickSight client
quicksight = boto3.client('quicksight')

def create_quicksight_dashboard():
    """Create a QuickSight dashboard programmatically"""
    
    account_id = '123456789012'
    dashboard_id = 'sales-dashboard-2025'
    
    # Define dashboard definition
    dashboard_definition = {
        "DataSetIdentifierDeclarations": [
            {
                "DataSetIdentifier": "sales_data",
                "DataSetArn": f"arn:aws:quicksight:us-east-1:{account_id}:dataset/sales_dataset"
            }
        ],
        "Sheets": [
            {
                "SheetId": "sheet_1",
                "Name": "Sales Overview",
                "Visuals": [
                    {
                        "BarChartVisual": {
                            "VisualId": "sales_by_region_chart",
                            "Title": {
                                "Visibility": "VISIBLE",
                                "FormatText": {
                                    "PlainText": "Sales by Region"
                                }
                            },
                            "Subtitle": {
                                "Visibility": "VISIBLE",
                                "FormatText": {
                                    "PlainText": "YTD Performance"
                                }
                            },
                            "ChartConfiguration": {
                                "FieldWells": {
                                    "BarChartAggregatedFieldWells": {
                                        "Category": [
                                            {
                                                "CategoricalDimensionField": {
                                                    "FieldId": "region",
                                                    "Column": {
                                                        "DataSetIdentifier": "sales_data",
                                                        "ColumnName": "region"
                                                    }
                                                }
                                            }
                                        ],
                                        "Values": [
                                            {
                                                "NumericalMeasureField": {
                                                    "FieldId": "sales_amount",
                                                    "Column": {
                                                        "DataSetIdentifier": "sales_data", 
                                                        "ColumnName": "sales_amount"
                                                    },
                                                    "AggregationFunction": {
                                                        "SimpleNumericalAggregation": "SUM"
                                                    }
                                                }
                                            }
                                        ]
                                    }
                                },
                                "SortConfiguration": {
                                    "CategorySort": [
                                        {
                                            "FieldSort": {
                                                "FieldId": "sales_amount",
                                                "Direction": "DESC"
                                            }
                                        }
                                    ]
                                },
                                "CategoryAxis": {
                                    "ScrollbarOptions": {
                                        "Visibility": "HIDDEN"
                                    }
                                },
                                "ValueAxis": {
                                    "ScrollbarOptions": {
                                        "Visibility": "HIDDEN"
                                    }
                                }
                            }
                        }
                    },
                    {
                        "LineChartVisual": {
                            "VisualId": "sales_trend_chart",
                            "Title": {
                                "Visibility": "VISIBLE",
                                "FormatText": {
                                    "PlainText": "Sales Trend"
                                }
                            },
                            "ChartConfiguration": {
                                "FieldWells": {
                                    "LineChartAggregatedFieldWells": {
                                        "Category": [
                                            {
                                                "DateDimensionField": {
                                                    "FieldId": "date",
                                                    "Column": {
                                                        "DataSetIdentifier": "sales_data",
                                                        "ColumnName": "date" 
                                                    },
                                                    "DateGranularity": "MONTH"
                                                }
                                            }
                                        ],
                                        "Values": [
                                            {
                                                "NumericalMeasureField": {
                                                    "FieldId": "monthly_sales",
                                                    "Column": {
                                                        "DataSetIdentifier": "sales_data",
                                                        "ColumnName": "sales_amount"
                                                    },
                                                    "AggregationFunction": {
                                                        "SimpleNumericalAggregation": "SUM"
                                                    }
                                                }
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        ]
    }
    
    # Create the dashboard
    try:
        response = quicksight.create_dashboard(
            AwsAccountId=account_id,
            DashboardId=dashboard_id,
            Name='Sales Performance Dashboard',
            Permissions=[
                {
                    'Principal': f'arn:aws:quicksight:us-east-1:{account_id}:user/default/admin',
                    'Actions': [
                        'quicksight:DescribeDashboard',
                        'quicksight:ListDashboardVersions', 
                        'quicksight:UpdateDashboardPermissions',
                        'quicksight:QueryDashboard',
                        'quicksight:UpdateDashboard',
                        'quicksight:DeleteDashboard',
                        'quicksight:DescribeDashboardPermissions',
                        'quicksight:UpdateDashboardPublishedVersion'
                    ]
                }
            ],
            DashboardPublishOptions={
                'AdHocFilteringOption': {
                    'AvailabilityStatus': 'ENABLED'
                },
                'ExportToCSVOption': {
                    'AvailabilityStatus': 'ENABLED'  
                },
                'SheetControlsOption': {
                    'VisibilityState': 'EXPANDED'
                }
            },
            Definition=dashboard_definition,
            Tags=[
                {
                    'Key': 'Environment',
                    'Value': 'Production'
                },
                {
                    'Key': 'Team', 
                    'Value': 'Analytics'
                }
            ]
        )
        
        print(f"Dashboard created successfully!")
        print(f"Dashboard ARN: {response['Arn']}")
        print(f"Dashboard URL: {response['DashboardVersionArn']}")
        return response
        
    except Exception as e:
        print(f"Error creating dashboard: {e}")
        return None

# Advanced dashboard with ML insights
def create_ml_dashboard():
    """Create dashboard with ML-powered insights"""
    
    ml_definition = {
        "DataSetIdentifierDeclarations": [
            {
                "DataSetIdentifier": "sales_forecast_data",
                "DataSetArn": f"arn:aws:quicksight:us-east-1:{account_id}:dataset/sales_forecast"
            }
        ],
        "Sheets": [
            {
                "SheetId": "ml_insights_sheet",
                "Name": "ML Insights",
                "Visuals": [
                    {
                        "InsightVisual": {
                            "VisualId": "anomaly_detection",
                            "Title": {
                                "Visibility": "VISIBLE",
                                "FormatText": {
                                    "PlainText": "Sales Anomaly Detection"
                                }
                            },
                            "DataSetIdentifier": "sales_forecast_data",
                            "InsightConfiguration": {
                                "Computations": [
                                    {
                                        "TopBottomRanked": {
                                            "ComputationId": "top_products",
                                            "Name": "Top Selling Products",
                                            "Category": {
                                                "FieldId": "product_category"
                                            },
                                            "Value": {
                                                "FieldId": "sales_amount"
                                            },
                                            "Type": "TOP",
                                            "ResultSize": 10
                                        }
                                    }
                                ]
                            }
                        }
                    },
                    {
                        "ForecastVisual": {
                            "VisualId": "sales_forecast",
                            "Title": {
                                "Visibility": "VISIBLE",
                                "FormatText": {
                                    "PlainText": "90-Day Sales Forecast"
                                }
                            },
                            "DataSetIdentifier": "sales_forecast_data",
                            "ForecastConfiguration": {
                                "ForecastProperties": {
                                    "PeriodsForward": 90,
                                    "PredictionInterval": 95,
                                    "Seasonality": "AUTOMATIC"
                                }
                            }
                        }
                    }
                ]
            }
        ]
    }
    
    return ml_definition

# Usage example
if __name__ == "__main__":
    dashboard = create_quicksight_dashboard()
    if dashboard:
        print("Dashboard ready for use!")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
import boto3
from botocore.exceptions import ClientError

def manage_quicksight_users():
    """Manage QuickSight users and permissions"""
    
    quicksight = boto3.client('quicksight')
    account_id = '123456789012'
    namespace = 'default'
    
    def create_user(username, email, role='READER'):
        """Create a new QuickSight user"""
        try:
            response = quicksight.register_user(
                IdentityType='QUICKSIGHT',
                Email=email,
                UserRole=role,  # READER, AUTHOR, ADMIN
                AwsAccountId=account_id,
                Namespace=namespace,
                UserName=username,
                SessionName=f'{username}-session'
            )
            return response['User']
        except ClientError as e:
            print(f"Error creating user {username}: {e}")
            return None
    
    def update_user_permissions(username, permissions):
        """Update user permissions for specific resources"""
        try:
            response = quicksight.update_user(
                UserName=username,
                AwsAccountId=account_id,
                Namespace=namespace,
                Email=permissions.get('email'),
                Role=permissions.get('role', 'READER'),
                CustomPermissionsName=permissions.get('custom_permissions')
            )
            return response['User']
        except ClientError as e:
            print(f"Error updating user {username}: {e}")
            return None
    
    def create_group(group_name, description=""):
        """Create user group for permission management"""
        try:
            response = quicksight.create_group(
                GroupName=group_name,
                Description=description,
                AwsAccountId=account_id,
                Namespace=namespace
            )
            return response['Group']
        except ClientError as e:
            print(f"Error creating group {group_name}: {e}")
            return None
    
    def add_user_to_group(username, group_name):
        """Add user to a group"""
        try:
            response = quicksight.create_group_membership(
                MemberName=username,
                GroupName=group_name,
                AwsAccountId=account_id,
                Namespace=namespace
            )
            return response['GroupMember']
        except ClientError as e:
            print(f"Error adding {username} to {group_name}: {e}")
            return None
    
    def list_users():
        """List all QuickSight users"""
        try:
            response = quicksight.list_users(
                AwsAccountId=account_id,
                Namespace=namespace
            )
            return response['UserList']
        except ClientError as e:
            print(f"Error listing users: {e}")
            return []
    
    def setup_data_analyst_permissions():
        """Set up permissions for data analysts"""
        
        # Create custom permissions for data analysts
        custom_permissions = {
            'name': 'DataAnalystPermissions',
            'actions': [
                'quicksight:CreateAnalysis',
                'quicksight:UpdateAnalysis', 
                'quicksight:DeleteAnalysis',
                'quicksight:DescribeAnalysis',
                'quicksight:SearchAnalyses',
                'quicksight:CreateDataSet',
                'quicksight:UpdateDataSet',
                'quicksight:DescribeDataSet',
                'quicksight:SearchDataSets',
                'quicksight:PassDataSet'
            ]
        }
        
        # Create the custom permission
        try:
            quicksight.put_data_set_refresh_properties(
                AwsAccountId=account_id,
                DataSetId='analyst-permissions',
                DataSetRefreshProperties={
                    'RefreshConfiguration': {
                        'IncrementalRefresh': {
                            'LookbackWindow': {
                                'ColumnName': 'created_date',
                                'Size': 1,
                                'SizeUnit': 'DAY'
                            }
                        }
                    }
                }
            )
        except Exception as e:
            print(f"Permission setup info: {e}")
    
    return {
        'create_user': create_user,
        'update_user_permissions': update_user_permissions,
        'create_group': create_group,
        'add_user_to_group': add_user_to_group,
        'list_users': list_users,
        'setup_permissions': setup_data_analyst_permissions
    }

# Example usage
def setup_analytics_team():
    """Set up a complete analytics team structure"""
    
    user_manager = manage_quicksight_users()
    
    # Create groups
    print("Creating user groups...")
    analyst_group = user_manager['create_group']('analysts', 'Data Analysts Team')
    viewer_group = user_manager['create_group']('viewers', 'Dashboard Viewers')
    admin_group = user_manager['create_group']('admins', 'QuickSight Administrators')
    
    # Create users
    print("Creating users...")
    users_to_create = [
        {'username': 'john.analyst', 'email': 'john@company.com', 'role': 'AUTHOR'},
        {'username': 'sarah.viewer', 'email': 'sarah@company.com', 'role': 'READER'},
        {'username': 'mike.admin', 'email': 'mike@company.com', 'role': 'ADMIN'}
    ]
    
    for user_info in users_to_create:
        user = user_manager['create_user'](
            user_info['username'], 
            user_info['email'], 
            user_info['role']
        )
        if user:
            print(f"Created user: {user['UserName']}")
            
            # Add to appropriate group
            if user_info['role'] == 'AUTHOR':
                user_manager['add_user_to_group'](user_info['username'], 'analysts')
            elif user_info['role'] == 'READER':
                user_manager['add_user_to_group'](user_info['username'], 'viewers')
            else:
                user_manager['add_user_to_group'](user_info['username'], 'admins')
    
    # Set up permissions
    user_manager['setup_permissions']()
    
    print("Analytics team setup complete!")

# Row-level security example
def setup_row_level_security():
    """Configure row-level security for data access"""
    
    quicksight = boto3.client('quicksight')
    account_id = '123456789012'
    
    # Create data set with RLS rules
    rls_rules = [
        {
            'RuleId': 'region-access-rule',
            'ColumnName': 'sales_region',
            'Expression': '${aws:userid} = sales_manager_north AND sales_region = "North America"'
        },
        {
            'RuleId': 'department-access-rule', 
            'ColumnName': 'department',
            'Expression': '${aws:PrincipalTag/Department} = department'
        }
    ]
    
    try:
        response = quicksight.create_data_set(
            AwsAccountId=account_id,
            DataSetId='secure-sales-data',
            Name='Secure Sales Dataset',
            ImportMode='SPICE',
            PhysicalTableMap={
                'sales_table': {
                    'S3Source': {
                        'DataSourceArn': f'arn:aws:quicksight:us-east-1:{account_id}:datasource/s3-sales-data',
                        'InputColumns': [
                            {'Name': 'sales_region', 'Type': 'STRING'},
                            {'Name': 'department', 'Type': 'STRING'},
                            {'Name': 'sales_amount', 'Type': 'DECIMAL'},
                            {'Name': 'employee_id', 'Type': 'STRING'}
                        ]
                    }
                }
            },
            RowLevelPermissionDataSet={
                'Namespace': 'default',
                'Arn': f'arn:aws:quicksight:us-east-1:{account_id}:dataset/rls-permissions',
                'PermissionPolicy': 'GRANT_ACCESS'
            }
        )
        print("RLS-enabled dataset created successfully!")
        return response
    except Exception as e:
        print(f"Error setting up RLS: {e}")
        return None

if __name__ == "__main__":
    setup_analytics_team()
    setup_row_level_security()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
import boto3
import json
from datetime import datetime

def manage_quicksight_data_sources():
    """Manage QuickSight data source connections"""
    
    quicksight = boto3.client('quicksight')
    account_id = '123456789012'
    
    def create_s3_data_source():
        """Create Amazon S3 data source"""
        try:
            response = quicksight.create_data_source(
                AwsAccountId=account_id,
                DataSourceId='s3-sales-data-source',
                Name='S3 Sales Data',
                Type='S3',
                DataSourceParameters={
                    'S3Parameters': {
                        'ManifestFileLocation': {
                            'Bucket': 'my-analytics-bucket',
                            'Key': 'sales-data/manifest.json'
                        }
                    }
                },
                Permissions=[
                    {
                        'Principal': f'arn:aws:quicksight:us-east-1:{account_id}:user/default/admin',
                        'Actions': [
                            'quicksight:UpdateDataSourcePermissions',
                            'quicksight:DescribeDataSource',
                            'quicksight:DescribeDataSourcePermissions',
                            'quicksight:PassDataSource',
                            'quicksight:UpdateDataSource',
                            'quicksight:DeleteDataSource'
                        ]
                    }
                ],
                SslProperties={
                    'DisableSsl': False
                },
                Tags=[
                    {'Key': 'Environment', 'Value': 'Production'},
                    {'Key': 'Team', 'Value': 'Analytics'}
                ]
            )
            return response
        except Exception as e:
            print(f"Error creating S3 data source: {e}")
            return None
    
    def create_redshift_data_source():
        """Create Amazon Redshift data source"""
        try:
            response = quicksight.create_data_source(
                AwsAccountId=account_id,
                DataSourceId='redshift-warehouse-source',
                Name='Redshift Data Warehouse',
                Type='REDSHIFT',
                DataSourceParameters={
                    'RedshiftParameters': {
                        'Host': 'redshift-cluster.abc123.us-west-2.redshift.amazonaws.com',
                        'Port': 5439,
                        'Database': 'analytics_db'
                    }
                },
                Credentials={
                    'CredentialPair': {
                        'Username': 'quicksight_user',
                        'Password': 'secure_password'  # Better to use Secrets Manager
                    }
                },
                VpcConnectionProperties={
                    'VpcConnectionArn': 'arn:aws:quicksight:us-west-2:123456789012:vpcConnection/vpc-123456'
                },
                SslProperties={
                    'DisableSsl': False
                }
            )
            return response
        except Exception as e:
            print(f"Error creating Redshift data source: {e}")
            return None
    
    def create_athena_data_source():
        """Create Amazon Athena data source"""
        try:
            response = quicksight.create_data_source(
                AwsAccountId=account_id,
                DataSourceId='athena-lakehouse-source',
                Name='Athena Data Lake',
                Type='ATHENA',
                DataSourceParameters={
                    'AthenaParameters': {
                        'WorkGroup': 'primary'
                    }
                },
                Permissions=[
                    {
                        'Principal': f'arn:aws:quicksight:us-east-1:{account_id}:group/default/analysts',
                        'Actions': [
                            'quicksight:DescribeDataSource',
                            'quicksight:PassDataSource'
                        ]
                    }
                ]
            )
            return response
        except Exception as e:
            print(f"Error creating Athena data source: {e}")
            return None
    
    def create_rds_mysql_source():
        """Create RDS MySQL data source"""
        try:
            response = quicksight.create_data_source(
                AwsAccountId=account_id,
                DataSourceId='rds-mysql-source',
                Name='MySQL Production Database',
                Type='MYSQL',
                DataSourceParameters={
                    'RdsParameters': {
                        'InstanceId': 'prod-mysql-instance',
                        'Database': 'sales_db'
                    }
                },
                Credentials={
                    'SecretArn': 'arn:aws:secretsmanager:us-east-1:123456789012:secret:rds-mysql-credentials'
                },
                SslProperties={
                    'DisableSsl': False
                }
            )
            return response
        except Exception as e:
            print(f"Error creating MySQL data source: {e}")
            return None
    
    def create_dataset_from_source(data_source_arn):
        """Create dataset from data source"""
        try:
            response = quicksight.create_data_set(
                AwsAccountId=account_id,
                DataSetId='multi-source-dataset',
                Name='Comprehensive Sales Dataset',
                ImportMode='SPICE',  # or 'DIRECT_QUERY'
                PhysicalTableMap={
                    'sales_fact_table': {
                        'RelationalTable': {
                            'DataSourceArn': data_source_arn,
                            'Catalog': 'AwsDataCatalog',
                            'Schema': 'sales',
                            'Name': 'fact_sales',
                            'InputColumns': [
                                {'Name': 'sale_id', 'Type': 'INTEGER'},
                                {'Name': 'customer_id', 'Type': 'INTEGER'},
                                {'Name': 'product_id', 'Type': 'INTEGER'},
                                {'Name': 'sale_date', 'Type': 'DATETIME'},
                                {'Name': 'amount', 'Type': 'DECIMAL'},
                                {'Name': 'quantity', 'Type': 'INTEGER'},
                                {'Name': 'region', 'Type': 'STRING'}
                            ]
                        }
                    },
                    'customer_dim_table': {
                        'RelationalTable': {
                            'DataSourceArn': data_source_arn,
                            'Schema': 'sales',
                            'Name': 'dim_customer',
                            'InputColumns': [
                                {'Name': 'customer_id', 'Type': 'INTEGER'},
                                {'Name': 'customer_name', 'Type': 'STRING'},
                                {'Name': 'customer_segment', 'Type': 'STRING'},
                                {'Name': 'registration_date', 'Type': 'DATETIME'}
                            ]
                        }
                    }
                },
                LogicalTableMap={
                    'sales_with_customer': {
                        'Alias': 'Sales with Customer Details',
                        'DataTransforms': [
                            {
                                'CreateColumnsOperation': {
                                    'Columns': [
                                        {
                                            'ColumnName': 'profit_margin',
                                            'ColumnId': 'profit_margin',
                                            'Expression': 'amount * 0.3'  # Calculated field
                                        },
                                        {
                                            'ColumnName': 'sale_year',
                                            'ColumnId': 'sale_year', 
                                            'Expression': 'extract("YYYY" from {sale_date})'
                                        }
                                    ]
                                }
                            },
                            {
                                'FilterOperation': {
                                    'ConditionExpression': 'amount > 0 AND quantity > 0'
                                }
                            }
                        ],
                        'Source': {
                            'JoinInstruction': {
                                'LeftOperand': 'sales_fact_table',
                                'RightOperand': 'customer_dim_table',
                                'Type': 'INNER',
                                'OnClause': 'sales_fact_table.customer_id = customer_dim_table.customer_id'
                            }
                        }
                    }
                },
                ImportMode='SPICE',
                ColumnGroups=[
                    {
                        'GeoSpatialColumnGroup': {
                            'Name': 'Location',
                            'CountryCode': 'US',
                            'Columns': ['region']
                        }
                    }
                ],
                Permissions=[
                    {
                        'Principal': f'arn:aws:quicksight:us-east-1:{account_id}:group/default/analysts',
                        'Actions': [
                            'quicksight:UpdateDataSetPermissions',
                            'quicksight:DescribeDataSet',
                            'quicksight:DescribeDataSetPermissions',
                            'quicksight:PassDataSet',
                            'quicksight:DescribeIngestion',
                            'quicksight:ListIngestions'
                        ]
                    }
                ],
                Tags=[
                    {'Key': 'DataType', 'Value': 'Sales'},
                    {'Key': 'RefreshFrequency', 'Value': 'Daily'}
                ]
            )
            return response
        except Exception as e:
            print(f"Error creating dataset: {e}")
            return None
    
    def setup_incremental_refresh(dataset_id):
        """Configure incremental refresh for dataset"""
        try:
            response = quicksight.put_data_set_refresh_properties(
                AwsAccountId=account_id,
                DataSetId=dataset_id,
                DataSetRefreshProperties={
                    'RefreshConfiguration': {
                        'IncrementalRefresh': {
                            'LookbackWindow': {
                                'ColumnName': 'sale_date',
                                'Size': 1,
                                'SizeUnit': 'DAY'
                            }
                        }
                    }
                }
            )
            return response
        except Exception as e:
            print(f"Error setting up incremental refresh: {e}")
            return None
    
    def monitor_data_refresh():
        """Monitor dataset refresh status"""
        try:
            response = quicksight.list_ingestions(
                DataSetId='multi-source-dataset',
                AwsAccountId=account_id
            )
            
            for ingestion in response['Ingestions']:
                print(f"Ingestion ID: {ingestion['IngestionId']}")
                print(f"Status: {ingestion['IngestionStatus']}")
                print(f"Started: {ingestion['CreatedTime']}")
                if 'ErrorInfo' in ingestion:
                    print(f"Error: {ingestion['ErrorInfo']}")
                print("-" * 40)
                
            return response
        except Exception as e:
            print(f"Error monitoring refresh: {e}")
            return None
    
    # Return all functions
    return {
        'create_s3_source': create_s3_data_source,
        'create_redshift_source': create_redshift_data_source,
        'create_athena_source': create_athena_data_source,
        'create_rds_source': create_rds_mysql_source,
        'create_dataset': create_dataset_from_source,
        'setup_incremental_refresh': setup_incremental_refresh,
        'monitor_refresh': monitor_data_refresh
    }

# Example: Complete data source setup
def setup_multi_source_analytics():
    """Set up multi-source analytics environment"""
    
    ds_manager = manage_quicksight_data_sources()
    
    print("Setting up data sources...")
    
    # Create data sources
    s3_source = ds_manager['create_s3_source']()
    redshift_source = ds_manager['create_redshift_source']()
    athena_source = ds_manager['create_athena_source']()
    
    if redshift_source:
        print("Creating dataset from Redshift...")
        dataset = ds_manager['create_dataset'](redshift_source['Arn'])
        
        if dataset:
            print("Setting up incremental refresh...")
            ds_manager['setup_incremental_refresh']('multi-source-dataset')
            
            print("Monitoring refresh status...")
            ds_manager['monitor_refresh']()
    
    print("Multi-source analytics setup complete!")

if __name__ == "__main__":
    setup_multi_source_analytics()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def accessing_data_quicksight_tab():
    """Content for Accessing data with QuickSight tab"""
    st.markdown("## 💾 Accessing data with QuickSight")
    st.markdown("*SPICE engine for faster processing, reduced wait time, and cost optimization through data reuse*")
    
    # Key concept about SPICE
    st.markdown('<div class="spice-card">', unsafe_allow_html=True)
    st.markdown("""
    ### ⚡ SPICE: Super-fast, Parallel, In-memory Calculation Engine
    
    **What is SPICE?**
    SPICE is QuickSight's powerful in-memory engine that provides:
    - **10x Faster Performance**: In-memory processing vs. direct queries
    - **Cost Optimization**: Reuse imported data multiple times without additional charges
    - **Parallel Processing**: Distribute calculations across multiple cores
    - **Automatic Optimization**: Smart caching and indexing
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # SPICE Architecture
    st.markdown("#### 🏗️ SPICE Engine Architecture")
    common.mermaid(create_spice_architecture(), height=550)
    
    # SPICE vs Direct Query Comparison
    st.markdown("#### ⚖️ SPICE vs Direct Query Comparison")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚡ SPICE (Recommended)
        
        **Advantages:**
        - 🚀 **Faster Queries**: Sub-second response times
        - 💰 **Cost Efficient**: No repeated charges for same data
        - 📊 **Advanced Analytics**: ML insights and complex calculations
        - 🔄 **Offline Access**: Works without source connectivity
        - 📈 **Better Concurrency**: Support for many simultaneous users
        
        **Best For:**
        - Frequently accessed dashboards
        - Complex analytical queries
        - Real-time user interactions
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔗 Direct Query
        
        **Advantages:**
        - 📡 **Real-time Data**: Always current information
        - 💾 **No Storage Limits**: Use source database capacity  
        - 🔄 **Auto Updates**: Data changes immediately reflected
        - 🏗️ **Simplified Architecture**: No data duplication
        
        **Limitations:**
        - ⏱️ **Slower Performance**: Query time depends on source
        - 💸 **Higher Costs**: Charges per query to source
        - 🚫 **Limited Features**: Some analytics features unavailable
        
        **Best For:**
        - Infrequently accessed reports
        - Real-time monitoring dashboards
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive SPICE calculator
    st.markdown("#### 🧮 SPICE Capacity Planning Calculator")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        data_size_gb = st.number_input("Data Size (GB)", min_value=0.1, max_value=1000.0, value=10.0, step=0.1)
        num_datasets = st.number_input("Number of Datasets", min_value=1, max_value=50, value=3)
        
    with col2:
        refresh_frequency = st.selectbox("Refresh Frequency", ["Hourly", "Daily", "Weekly", "Monthly"])
        compression_ratio = st.slider("Expected Compression Ratio", 0.2, 0.8, 0.4, 0.1)
    
    with col3:
        num_users = st.number_input("Concurrent Users", min_value=1, max_value=1000, value=25)
        query_complexity = st.selectbox("Query Complexity", ["Simple", "Medium", "Complex"])
    
    # Calculate SPICE requirements
    spice_capacity, cost_estimate, performance_gain = calculate_spice_metrics(
        data_size_gb, num_datasets, refresh_frequency, compression_ratio, num_users, query_complexity
    )
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📊 SPICE Capacity Recommendation
    **Required SPICE Capacity**: {spice_capacity:.2f} GB  
    **Estimated Monthly Cost**: ${cost_estimate:.2f}  
    **Expected Performance Gain**: {performance_gain}x faster  
    **Recommended Refresh**: {refresh_frequency} during off-peak hours  
    **Storage Efficiency**: {compression_ratio*100:.0f}% compression expected
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Data source connectivity showcase
    st.markdown("#### 🔌 Supported Data Sources")
    
    # Create comprehensive data sources matrix
    data_sources_info = create_data_sources_matrix()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### ☁️ AWS Native Sources")
        aws_sources = data_sources_info[data_sources_info['Category'] == 'AWS']
        st.dataframe(aws_sources[['Source', 'SPICE Support', 'Real-time']], hide_index=True, use_container_width=True)
    
    with col2:
        st.markdown("##### 🏢 External Sources") 
        external_sources = data_sources_info[data_sources_info['Category'] == 'External']
        st.dataframe(external_sources[['Source', 'SPICE Support', 'Real-time']], hide_index=True, use_container_width=True)
    
    # SPICE refresh strategies
    st.markdown("#### 🔄 SPICE Refresh Strategies")
    
    tab1, tab2, tab3 = st.tabs(["Full Refresh", "Incremental Refresh", "Real-time Refresh"])
    
    with tab1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔄 Full Refresh Strategy
        
        **When to Use:**
        - Small to medium datasets (< 1GB)
        - Data with frequent schema changes
        - Historical data that doesn't change
        - Initial data loads
        
        **Characteristics:**
        - ✅ Complete data replacement
        - ✅ Ensures data consistency
        - ✅ Handles schema changes automatically
        - ⚠️ Longer refresh times for large datasets
        - ⚠️ Higher source database load
        
        **Best Practices:**
        - Schedule during off-peak hours
        - Monitor refresh completion
        - Set up failure notifications
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
import boto3
from datetime import datetime, timedelta

def schedule_full_refresh():
    """Schedule full refresh for SPICE datasets"""
    
    quicksight = boto3.client('quicksight')
    events = boto3.client('events')
    
    account_id = '123456789012'
    dataset_id = 'sales-dataset'
    
    # Create ingestion job
    def trigger_full_refresh():
        try:
            response = quicksight.create_ingestion(
                DataSetId=dataset_id,
                IngestionId=f'full-refresh-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
                AwsAccountId=account_id,
                IngestionType='FULL_REFRESH'  # Complete data replacement
            )
            
            print(f"Full refresh initiated: {response['IngestionId']}")
            return response['IngestionArn']
            
        except Exception as e:
            print(f"Error initiating refresh: {e}")
            return None
    
    # Monitor refresh progress
    def monitor_refresh_status(ingestion_id):
        try:
            response = quicksight.describe_ingestion(
                DataSetId=dataset_id,
                IngestionId=ingestion_id,
                AwsAccountId=account_id
            )
            
            ingestion = response['Ingestion']
            status = ingestion['IngestionStatus']
            
            print(f"Refresh Status: {status}")
            print(f"Rows Ingested: {ingestion.get('RowInfo', {}).get('RowsIngested', 0)}")
            print(f"Rows Dropped: {ingestion.get('RowInfo', {}).get('RowsDropped', 0)}")
            
            if status == 'COMPLETED':
                print("✅ Full refresh completed successfully")
            elif status == 'FAILED':
                print(f"❌ Refresh failed: {ingestion.get('ErrorInfo', {})}")
                
            return status
            
        except Exception as e:
            print(f"Error checking status: {e}")
            return 'UNKNOWN'
    
    # Set up automated schedule using EventBridge
    def create_refresh_schedule():
        rule_name = 'quicksight-daily-refresh'
        
        # Create EventBridge rule for daily 2 AM refresh
        events.put_rule(
            Name=rule_name,
            ScheduleExpression='cron(0 2 * * ? *)',  # Daily at 2 AM UTC
            Description='Daily QuickSight dataset refresh',
            State='ENABLED'
        )
        
        # Add QuickSight as target (requires Lambda for API call)
        events.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    'Id': '1',
                    'Arn': f'arn:aws:lambda:us-east-1:{account_id}:function:quicksight-refresh-trigger',
                    'Input': json.dumps({
                        'dataset_id': dataset_id,
                        'refresh_type': 'FULL_REFRESH'
                    })
                }
            ]
        )
        
        print("Automated refresh schedule created")
    
    # Execute full refresh
    ingestion_arn = trigger_full_refresh()
    if ingestion_arn:
        # Create schedule for future refreshes
        create_refresh_schedule()
        return True
    return False

# Lambda function for automated refresh
def lambda_handler(event, context):
    """AWS Lambda function to trigger QuickSight refresh"""
    
    import json
    
    quicksight = boto3.client('quicksight')
    
    dataset_id = event['dataset_id']
    refresh_type = event['refresh_type']
    account_id = context.invoked_function_arn.split(':')[4]
    
    try:
        response = quicksight.create_ingestion(
            DataSetId=dataset_id,
            IngestionId=f'automated-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
            AwsAccountId=account_id,
            IngestionType=refresh_type
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Refresh initiated successfully',
                'ingestion_id': response['IngestionId']
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

if __name__ == "__main__":
    success = schedule_full_refresh()
    print(f"Refresh setup: {'Success' if success else 'Failed'}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚡ Incremental Refresh Strategy
        
        **When to Use:**
        - Large datasets (> 1GB)
        - Frequently updated transactional data
        - Time-series data with new records
        - Cost-sensitive environments
        
        **Requirements:**
        - ✅ Incremental column (timestamp, date, or auto-incrementing ID)
        - ✅ Data source supports filtering by incremental column
        - ✅ Predictable data arrival patterns
        
        **Benefits:**
        - 🚀 Faster refresh times (minutes vs hours)
        - 💰 Lower source database impact
        - ⚡ Reduced network transfer
        - 🔄 More frequent updates possible
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
import boto3
from datetime import datetime, timedelta

def setup_incremental_refresh():
    """Configure incremental refresh for large datasets"""
    
    quicksight = boto3.client('quicksight')
    account_id = '123456789012'
    dataset_id = 'large-transactions-dataset'
    
    # Configure incremental refresh properties
    def configure_incremental_properties():
        try:
            response = quicksight.put_data_set_refresh_properties(
                AwsAccountId=account_id,
                DataSetId=dataset_id,
                DataSetRefreshProperties={
                    'RefreshConfiguration': {
                        'IncrementalRefresh': {
                            'LookbackWindow': {
                                'ColumnName': 'transaction_timestamp',  # Incremental column
                                'Size': 1,                              # Look back 1 unit
                                'SizeUnit': 'HOUR'                      # Hours (DAY, HOUR, MINUTE)
                            }
                        }
                    }
                }
            )
            
            print("✅ Incremental refresh configured successfully")
            return response
            
        except Exception as e:
            print(f"Error configuring incremental refresh: {e}")
            return None
    
    # Create dataset optimized for incremental refresh
    def create_incremental_dataset():
        try:
            response = quicksight.create_data_set(
                AwsAccountId=account_id,
                DataSetId=dataset_id,
                Name='Large Transactions Dataset',
                ImportMode='SPICE',
                PhysicalTableMap={
                    'transactions_table': {
                        'RelationalTable': {
                            'DataSourceArn': f'arn:aws:quicksight:us-east-1:{account_id}:datasource/transactions-db',
                            'Schema': 'public',
                            'Name': 'transactions',
                            'InputColumns': [
                                {'Name': 'transaction_id', 'Type': 'INTEGER'},
                                {'Name': 'customer_id', 'Type': 'INTEGER'},
                                {'Name': 'amount', 'Type': 'DECIMAL'},
                                {'Name': 'transaction_timestamp', 'Type': 'DATETIME'},  # Key for incremental
                                {'Name': 'status', 'Type': 'STRING'},
                                {'Name': 'created_date', 'Type': 'DATETIME'}
                            ]
                        }
                    }
                },
                LogicalTableMap={
                    'filtered_transactions': {
                        'Alias': 'Recent Transactions',
                        'DataTransforms': [
                            {
                                'FilterOperation': {
                                    'ConditionExpression': 'transaction_timestamp >= now() - interval \'7\' day'
                                }
                            },
                            {
                                'CreateColumnsOperation': {
                                    'Columns': [
                                        {
                                            'ColumnName': 'transaction_hour',
                                            'ColumnId': 'transaction_hour',
                                            'Expression': 'extract("HH" from {transaction_timestamp})'
                                        }
                                    ]
                                }
                            }
                        ],
                        'Source': {
                            'PhysicalTableId': 'transactions_table'
                        }
                    }
                }
            )
            
            print("✅ Incremental dataset created successfully")
            return response
            
        except Exception as e:
            print(f"Error creating incremental dataset: {e}")
            return None
    
    # Trigger incremental refresh
    def trigger_incremental_refresh():
        try:
            response = quicksight.create_ingestion(
                DataSetId=dataset_id,
                IngestionId=f'incremental-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
                AwsAccountId=account_id,
                IngestionType='INCREMENTAL_REFRESH'
            )
            
            print(f"✅ Incremental refresh started: {response['IngestionId']}")
            return response
            
        except Exception as e:
            print(f"Error starting incremental refresh: {e}")
            return None
    
    # Monitor incremental refresh efficiency
    def analyze_refresh_performance():
        try:
            # Get recent ingestions
            response = quicksight.list_ingestions(
                DataSetId=dataset_id,
                AwsAccountId=account_id,
                MaxResults=10
            )
            
            print("📊 Incremental Refresh Performance Analysis")
            print("=" * 50)
            
            for ingestion in response['Ingestions']:
                ingestion_type = ingestion.get('IngestionType', 'UNKNOWN')
                status = ingestion['IngestionStatus']
                created_time = ingestion['CreatedTime']
                
                if 'CompletedTime' in ingestion:
                    duration = (ingestion['CompletedTime'] - created_time).total_seconds()
                    print(f"Type: {ingestion_type}")
                    print(f"Duration: {duration:.1f} seconds")
                    print(f"Status: {status}")
                    
                    if 'RowInfo' in ingestion:
                        rows_ingested = ingestion['RowInfo'].get('RowsIngested', 0)
                        rows_dropped = ingestion['RowInfo'].get('RowsDropped', 0)
                        print(f"Rows Processed: {rows_ingested:,}")
                        print(f"Rows Dropped: {rows_dropped:,}")
                        
                        if duration > 0:
                            throughput = rows_ingested / duration
                            print(f"Throughput: {throughput:.0f} rows/second")
                    
                    print("-" * 30)
                    
            return response
            
        except Exception as e:
            print(f"Error analyzing performance: {e}")
            return None
    
    # Optimize incremental refresh window
    def optimize_refresh_window():
        """Analyze and optimize the lookback window"""
        
        # Example: Query to analyze data arrival patterns
        analysis_query = """
        SELECT 
            DATE_TRUNC('hour', transaction_timestamp) as hour,
            COUNT(*) as transaction_count,
            MIN(created_date) as first_created,
            MAX(created_date) as last_created,
            AVG(EXTRACT(EPOCH FROM (created_date - transaction_timestamp))) as avg_delay_seconds
        FROM transactions 
        WHERE transaction_timestamp >= NOW() - INTERVAL '7 days'
        GROUP BY DATE_TRUNC('hour', transaction_timestamp)
        ORDER BY hour DESC
        LIMIT 168;  -- 7 days * 24 hours
        """
        
        recommendations = {
            'current_window': '1 HOUR',
            'analysis': {
                'avg_data_delay': '15 minutes',
                'max_delay_observed': '45 minutes', 
                'recommended_window': '2 HOURS',
                'reasoning': 'Increase window to 2 hours to capture delayed transactions'
            }
        }
        
        print("🔍 Incremental Window Analysis")
        print("=" * 40)
        print(f"Current Window: {recommendations['current_window']}")
        print(f"Average Delay: {recommendations['analysis']['avg_data_delay']}")
        print(f"Max Delay: {recommendations['analysis']['max_delay_observed']}")
        print(f"Recommended: {recommendations['analysis']['recommended_window']}")
        print(f"Reason: {recommendations['analysis']['reasoning']}")
        
        return recommendations
    
    # Execute incremental refresh setup
    print("🚀 Setting up incremental refresh...")
    
    dataset = create_incremental_dataset()
    if dataset:
        properties = configure_incremental_properties()
        if properties:
            refresh = trigger_incremental_refresh()
            if refresh:
                print("\n📊 Performance Analysis:")
                analyze_refresh_performance()
                
                print("\n🔍 Window Optimization:")
                optimize_refresh_window()
                
                return True
    
    return False

# Advanced incremental refresh with partitioning
def setup_partitioned_incremental_refresh():
    """Set up incremental refresh with data partitioning"""
    
    quicksight = boto3.client('quicksight')
    account_id = '123456789012'
    
    # Create partitioned dataset
    partitioned_config = {
        'PhysicalTableMap': {
            'sales_2024': {
                'RelationalTable': {
                    'DataSourceArn': f'arn:aws:quicksight:us-east-1:{account_id}:datasource/partitioned-db',
                    'Schema': 'sales',
                    'Name': 'sales_fact',
                    'InputColumns': [
                        {'Name': 'sale_id', 'Type': 'INTEGER'},
                        {'Name': 'sale_date', 'Type': 'DATETIME'}, 
                        {'Name': 'amount', 'Type': 'DECIMAL'},
                        {'Name': 'partition_year', 'Type': 'INTEGER'},
                        {'Name': 'partition_month', 'Type': 'INTEGER'}
                    ]
                }
            }
        },
        'LogicalTableMap': {
            'current_year_sales': {
                'Source': {
                    'PhysicalTableId': 'sales_2024'
                },
                'DataTransforms': [
                    {
                        'FilterOperation': {
                            'ConditionExpression': 'partition_year = 2024 AND sale_date >= now() - interval \'1\' day'
                        }
                    }
                ]
            }
        }
    }
    
    print("✅ Partitioned incremental refresh configuration ready")
    return partitioned_config

if __name__ == "__main__":
    success = setup_incremental_refresh()
    print(f"\nIncremental refresh setup: {'✅ Success' if success else '❌ Failed'}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔴 Real-time Refresh Strategy
        
        **When to Use:**
        - Mission-critical dashboards
        - Real-time monitoring scenarios
        - Trading/financial applications
        - IoT sensor data visualization
        
        **Implementation Options:**
        - 🔄 **Frequent SPICE Refresh**: Every 15-60 minutes
        - 🔗 **Direct Query Mode**: Real-time but slower performance
        - 🌊 **Streaming Integration**: Amazon Kinesis + QuickSight Q
        - ⚡ **Event-Driven Refresh**: Triggered by data changes
        
        **Trade-offs:**
        - ✅ Always current data
        - ⚠️ Higher costs and complexity
        - ⚠️ Source system impact
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
import boto3
from datetime import datetime
import json

def setup_realtime_refresh():
    """Set up near real-time refresh for critical dashboards"""
    
    quicksight = boto3.client('quicksight')
    events = boto3.client('events')
    lambda_client = boto3.client('lambda')
    
    account_id = '123456789012'
    dataset_id = 'realtime-dashboard-data'
    
    # Option 1: High-frequency scheduled refresh
    def create_frequent_refresh_schedule():
        """Create schedule for every 15 minutes refresh"""
        
        rule_name = 'quicksight-realtime-refresh'
        
        # Create EventBridge rule for every 15 minutes
        events.put_rule(
            Name=rule_name,
            ScheduleExpression='rate(15 minutes)',
            Description='Real-time QuickSight dataset refresh',
            State='ENABLED'
        )
        
        # Target Lambda function
        events.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    'Id': '1',
                    'Arn': f'arn:aws:lambda:us-east-1:{account_id}:function:quicksight-realtime-refresh',
                    'Input': json.dumps({
                        'dataset_id': dataset_id,
                        'refresh_type': 'INCREMENTAL_REFRESH',
                        'priority': 'HIGH'
                    })
                }
            ]
        )
        
        print("✅ High-frequency refresh schedule created (15 min intervals)")
    
    # Option 2: Event-driven refresh
    def setup_event_driven_refresh():
        """Trigger refresh based on data changes"""
        
        # Create CloudTrail rule for S3 object creation
        s3_rule = {
            'Name': 'quicksight-s3-data-arrival',
            'EventPattern': json.dumps({
                "source": ["aws.s3"],
                "detail-type": ["Object Created"],
                "detail": {
                    "bucket": {"name": ["analytics-data-bucket"]},
                    "object": {"key": [{"prefix": "incoming-data/"}]}
                }
            }),
            'State': 'ENABLED',
            'Description': 'Trigger QuickSight refresh on new data arrival'
        }
        
        events.put_rule(**s3_rule)
        
        # Target for S3 events
        events.put_targets(
            Rule='quicksight-s3-data-arrival',
            Targets=[
                {
                    'Id': '1',
                    'Arn': f'arn:aws:lambda:us-east-1:{account_id}:function:quicksight-event-refresh',
                    'Input': json.dumps({
                        'dataset_id': dataset_id,
                        'trigger_type': 'DATA_ARRIVAL',
                        'refresh_type': 'INCREMENTAL_REFRESH'
                    })
                }
            ]
        )
        
        print("✅ Event-driven refresh configured for S3 data arrival")
    
    # Option 3: Direct query for ultra real-time
    def create_direct_query_dataset():
        """Create dataset with direct query for real-time data"""
        
        try:
            response = quicksight.create_data_set(
                AwsAccountId=account_id,
                DataSetId='realtime-direct-query',
                Name='Real-time Direct Query Dataset',
                ImportMode='DIRECT_QUERY',  # No SPICE - query source directly
                PhysicalTableMap={
                    'realtime_table': {
                        'RelationalTable': {
                            'DataSourceArn': f'arn:aws:quicksight:us-east-1:{account_id}:datasource/realtime-db',
                            'Schema': 'public',
                            'Name': 'live_metrics',
                            'InputColumns': [
                                {'Name': 'timestamp', 'Type': 'DATETIME'},
                                {'Name': 'metric_name', 'Type': 'STRING'},
                                {'Name': 'metric_value', 'Type': 'DECIMAL'},
                                {'Name': 'source_system', 'Type': 'STRING'}
                            ]
                        }
                    }
                },
                LogicalTableMap={
                    'recent_metrics': {
                        'Source': {
                            'PhysicalTableId': 'realtime_table'
                        },
                        'DataTransforms': [
                            {
                                'FilterOperation': {
                                    'ConditionExpression': 'timestamp >= now() - interval \'1\' hour'
                                }
                            }
                        ]
                    }
                }
            )
            
            print("✅ Direct query dataset created for ultra real-time data")
            return response
            
        except Exception as e:
            print(f"Error creating direct query dataset: {e}")
            return None
    
    # Option 4: Streaming integration
    def setup_streaming_integration():
        """Integrate with Kinesis for streaming data"""
        
        kinesis_config = {
            'stream_name': 'quicksight-realtime-stream',
            'delivery_stream': 'quicksight-data-firehose',
            'destination': {
                'type': 'S3',
                'bucket': 'quicksight-streaming-data',
                'prefix': 'year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/',
                'buffer_interval': 60,  # 1 minute
                'buffer_size': 1        # 1 MB
            }
        }
        
        # Create Kinesis Data Firehose delivery stream
        firehose = boto3.client('firehose')
        
        try:
            firehose.create_delivery_stream(
                DeliveryStreamName=kinesis_config['delivery_stream'],
                DeliveryStreamType='DirectPut',
                S3DestinationConfiguration={
                    'RoleArn': f'arn:aws:iam::{account_id}:role/firehose-delivery-role',
                    'BucketARN': f'arn:aws:s3:::{kinesis_config["destination"]["bucket"]}',
                    'Prefix': kinesis_config['destination']['prefix'],
                    'BufferingHints': {
                        'SizeInMBs': kinesis_config['destination']['buffer_size'],
                        'IntervalInSeconds': kinesis_config['destination']['buffer_interval']
                    },
                    'CompressionFormat': 'GZIP'
                }
            )
            
            print("✅ Kinesis Firehose delivery stream created")
            
            # Schedule frequent refresh of streaming data
            events.put_rule(
                Name='quicksight-streaming-refresh',
                ScheduleExpression='rate(2 minutes)',
                Description='Refresh QuickSight streaming dataset',
                State='ENABLED'
            )
            
            print("✅ Streaming integration configured")
            return True
            
        except Exception as e:
            print(f"Error setting up streaming: {e}")
            return False
    
    # Smart refresh orchestrator
    def create_smart_refresh_lambda():
        """Lambda function that intelligently manages refresh frequency"""
        
        lambda_code = """
import boto3
import json
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """Smart refresh orchestrator"""
    
    quicksight = boto3.client('quicksight')
    cloudwatch = boto3.client('cloudwatch')
    
    dataset_id = event['dataset_id']
    account_id = context.invoked_function_arn.split(':')[4]
    
    # Check dashboard usage before refreshing
    def check_dashboard_activity():
        try:
            # Get dashboard usage metrics
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/QuickSight',
                MetricName='SessionCount',
                Dimensions=[
                    {'Name': 'DataSetId', 'Value': dataset_id}
                ],
                StartTime=datetime.utcnow() - timedelta(minutes=30),
                EndTime=datetime.utcnow(),
                Period=300,
                Statistics=['Sum']
            )
            
            total_sessions = sum(point['Sum'] for point in response['Datapoints'])
            return total_sessions > 0
            
        except Exception as e:
            print(f"Error checking activity: {e}")
            return True  # Default to refresh if unable to check
    
    # Dynamic refresh based on activity
    if check_dashboard_activity():
        # High activity - use incremental refresh
        refresh_type = 'INCREMENTAL_REFRESH'
        print("High activity detected - using incremental refresh")
    else:
        # Low activity - skip refresh to save costs
        print("Low activity - skipping refresh")
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Refresh skipped due to low activity'})
        }
    
    try:
        response = quicksight.create_ingestion(
            DataSetId=dataset_id,
            IngestionId=f'smart-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
            AwsAccountId=account_id,
            IngestionType=refresh_type
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Smart refresh initiated',
                'refresh_type': refresh_type,
                'ingestion_id': response['IngestionId']
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
        """
        
        # Create Lambda function
        try:
            lambda_client.create_function(
                FunctionName='quicksight-smart-refresh',
                Runtime='python3.9',
                Role=f'arn:aws:iam::{account_id}:role/quicksight-lambda-role',
                Handler='lambda_function.lambda_handler',
                Code={'ZipFile': lambda_code.encode()},
                Description='Smart refresh orchestrator for QuickSight',
                Timeout=300,
                MemorySize=256,
                Environment={
                    'Variables': {
                        'ACCOUNT_ID': account_id,
                        'REGION': 'us-east-1'
                    }
                }
            )
            
            print("✅ Smart refresh Lambda function created")
            return True
            
        except Exception as e:
            print(f"Error creating Lambda function: {e}")
            return False
    
    # Execute real-time setup
    print("🚀 Setting up real-time refresh strategies...")
    
    create_frequent_refresh_schedule()
    setup_event_driven_refresh()
    direct_query_ds = create_direct_query_dataset()
    streaming_setup = setup_streaming_integration()
    smart_lambda = create_smart_refresh_lambda()
    
    print("\n📊 Real-time Refresh Summary:")
    print(f"✅ Frequent Schedule: Every 15 minutes")
    print(f"✅ Event-Driven: S3 data arrival triggers")
    print(f"✅ Direct Query: {'Created' if direct_query_ds else 'Failed'}")
    print(f"✅ Streaming: {'Configured' if streaming_setup else 'Failed'}")
    print(f"✅ Smart Orchestrator: {'Deployed' if smart_lambda else 'Failed'}")
    
    return {
        'frequent_schedule': True,
        'event_driven': True,
        'direct_query': bool(direct_query_ds),
        'streaming': streaming_setup,
        'smart_orchestrator': smart_lambda
    }

# Usage example
if __name__ == "__main__":
    result = setup_realtime_refresh()
    print(f"\nReal-time refresh capabilities: {sum(result.values())}/5 configured")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def quicksight_embedded_analytics_tab():
    """Content for QuickSight embedded analytics tab"""
    st.markdown("## 🔗 QuickSight Embedded Analytics")
    st.markdown("*Integrate interactive dashboards into applications with serverless scaling and pay-per-session pricing*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Embedded Analytics Overview
    QuickSight embedded analytics enables you to:
    - **Seamless Integration**: Embed dashboards into web and mobile applications
    - **Serverless Scaling**: Automatically scale from hundreds to millions of users
    - **Pay-per-Session**: Cost-effective pricing model - only pay when users access
    - **Custom Authentication**: Integrate with your existing identity providers
    - **White-label Experience**: Customizable branding and user interface
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Embedded analytics architecture
    st.markdown("#### 🏗️ Embedded Analytics Architecture")
    common.mermaid(create_embedded_analytics_flow(), height=650)
    
    # Integration steps
    st.markdown("#### 🔧 4-Step Integration Process")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 1️⃣ Create Dashboard
        - **Design in QuickSight Console**
        - **Configure visualizations**
        - **Set up data sources**
        - **Test functionality**
        - **Optimize for embedding**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 2️⃣ Apply Permissions
        - **Create IAM roles**
        - **Set dashboard permissions**
        - **Configure access policies**
        - **Set up row-level security**
        - **Define user groups**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 3️⃣ Authenticate App Server
        - **AWS credential configuration**
        - **Assume IAM roles**
        - **Generate embed URLs**
        - **Handle token refresh**
        - **Secure API calls**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col4:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 4️⃣ Embed via JavaScript SDK
        - **Include QuickSight SDK**
        - **Initialize embed container**
        - **Handle user interactions**
        - **Manage responsive design**
        - **Monitor performance**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive embedding configurator
    st.markdown("#### ⚙️ Embedding Configuration Simulator")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### Application Settings")
        app_type = st.selectbox("Application Type", ["Web Application", "Mobile App", "Portal/Intranet", "SaaS Platform"])
        auth_method = st.selectbox("Authentication Method", ["SAML SSO", "Active Directory", "Amazon Cognito", "Custom OAuth"])
        user_capacity = st.selectbox("Expected Users", ["< 100", "100-1,000", "1,000-10,000", "> 10,000"])
        
    with col2:
        st.markdown("##### Dashboard Settings")
        dashboard_type = st.selectbox("Dashboard Type", ["Executive Summary", "Operational Metrics", "Customer Analytics", "Financial Reports"])
        embedding_features = st.multiselect("Features to Enable", [
            "Export to PDF", "Email Reports", "Filtering", "Drill-down", "Real-time Updates"
        ], default=["Filtering", "Drill-down"])
        branding = st.selectbox("Branding Level", ["Full White-label", "Minimal QuickSight Branding", "Standard QuickSight"])
    
    # Calculate configuration recommendations
    config_estimate = calculate_embedding_config(app_type, auth_method, user_capacity, dashboard_type, embedding_features, branding)
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📊 Embedding Configuration Recommendation
    **Architecture Pattern**: {config_estimate['pattern']}  
    **Est. Monthly Cost**: ${config_estimate['cost_estimate']}  
    **Implementation Effort**: {config_estimate['effort']} days  
    **Authentication Setup**: {config_estimate['auth_complexity']}  
    **Scaling Approach**: {config_estimate['scaling']}
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Pricing model explanation
    st.markdown("#### 💰 Pay-per-Session Pricing Model")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 💡 How Pay-per-Session Works
        
        **Session Definition:**
        - A session starts when a user opens an embedded dashboard
        - Session continues for 30 minutes of inactivity
        - Multiple dashboards in same session = 1 charge
        
        **Pricing Tiers:**
        - **0-10,000 sessions/month**: $0.30 per session
        - **10,001-100,000 sessions/month**: $0.20 per session  
        - **100,001+ sessions/month**: $0.10 per session
        
        **Cost Benefits:**
        - No upfront costs or monthly minimums
        - Pay only for actual usage
        - Automatic volume discounts
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        # Interactive pricing calculator
        st.markdown("##### 🧮 Session Cost Calculator")
        
        monthly_sessions = st.number_input("Monthly Sessions", min_value=1, max_value=1000000, value=5000, step=100)
        avg_session_duration = st.slider("Avg Session Duration (minutes)", 5, 60, 15)
        
        # Calculate tiered pricing
        tier1_sessions = min(monthly_sessions, 10000)
        tier2_sessions = min(max(monthly_sessions - 10000, 0), 90000)
        tier3_sessions = max(monthly_sessions - 100000, 0)
        
        total_cost = (tier1_sessions * 0.30) + (tier2_sessions * 0.20) + (tier3_sessions * 0.10)
        cost_per_session = total_cost / monthly_sessions if monthly_sessions > 0 else 0
        
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f"""
        **Monthly Cost Breakdown:**  
        First 10K sessions: ${tier1_sessions * 0.30:.2f}  
        Next 90K sessions: ${tier2_sessions * 0.20:.2f}  
        Over 100K sessions: ${tier3_sessions * 0.10:.2f}  
        
        **Total: ${total_cost:.2f}/month**  
        **Average: ${cost_per_session:.3f}/session**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 Implementation Examples")
    
    tab1, tab2, tab3 = st.tabs(["Backend Integration", "Frontend JavaScript", "Authentication Setup"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
import boto3
import json
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from functools import wraps

# Flask application for QuickSight embedding
app = Flask(__name__)

# Initialize AWS clients
quicksight = boto3.client('quicksight', region_name='us-east-1')
sts = boto3.client('sts')

# Configuration
AWS_ACCOUNT_ID = '123456789012'
QUICKSIGHT_NAMESPACE = 'default'
EMBEDDING_ROLE_ARN = 'arn:aws:iam::123456789012:role/QuickSight-Embedding-Role'

class QuickSightEmbedding:
    """QuickSight embedding service"""
    
    def __init__(self, account_id, namespace):
        self.account_id = account_id
        self.namespace = namespace
        self.quicksight = boto3.client('quicksight')
    
    def get_dashboard_embed_url(self, dashboard_id, user_arn, session_name=None, reset_disabled=True):
        """Generate dashboard embedding URL"""
        
        if not session_name:
            session_name = f"embedded-session-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        try:
            response = self.quicksight.generate_embed_url_for_registered_user(
                AwsAccountId=self.account_id,
                SessionLifetimeInMinutes=600,  # 10 hours
                UserArn=user_arn,
                ExperienceConfiguration={
                    'Dashboard': {
                        'InitialDashboardId': dashboard_id,
                        'FeatureConfigurations': {
                            'StatePersistence': {
                                'Enabled': True
                            }
                        }
                    }
                },
                AllowedDomains=['https://yourdomain.com'],  # Restrict to your domains
                SessionTags=[
                    {
                        'Key': 'Department',
                        'Value': 'Analytics'
                    }
                ]
            )
            
            return {
                'success': True,
                'embed_url': response['EmbedUrl'],
                'request_id': response['RequestId']
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_anonymous_embed_url(self, dashboard_id, allowed_domains, namespace='default'):
        """Generate anonymous embedding URL (for public dashboards)"""
        
        try:
            response = self.quicksight.generate_embed_url_for_anonymous_user(
                AwsAccountId=self.account_id,
                Namespace=namespace,
                SessionLifetimeInMinutes=600,
                AuthorizedResourceArns=[
                    f'arn:aws:quicksight:us-east-1:{self.account_id}:dashboard/{dashboard_id}'
                ],
                ExperienceConfiguration={
                    'Dashboard': {
                        'InitialDashboardId': dashboard_id,
                        'FeatureConfigurations': {
                            'StatePersistence': {
                                'Enabled': False  # Disabled for anonymous users
                            }
                        }
                    }
                },
                AllowedDomains=allowed_domains,
                SessionTags=[
                    {
                        'Key': 'AccessType',
                        'Value': 'Anonymous'
                    }
                ]
            )
            
            return {
                'success': True,
                'embed_url': response['EmbedUrl'],
                'request_id': response['RequestId']
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def create_or_update_user(self, username, email, role='READER', session_name=None):
        """Create or update QuickSight user for embedding"""
        
        if not session_name:
            session_name = f"{username}-session"
        
        try:
            # Try to describe existing user first
            try:
                user_response = self.quicksight.describe_user(
                    UserName=username,
                    AwsAccountId=self.account_id,
                    Namespace=self.namespace
                )
                user_arn = user_response['User']['Arn']
                print(f"Using existing user: {user_arn}")
                
            except self.quicksight.exceptions.ResourceNotFoundException:
                # User doesn't exist, create new one
                create_response = self.quicksight.register_user(
                    IdentityType='QUICKSIGHT',
                    Email=email,
                    UserRole=role,
                    AwsAccountId=self.account_id,
                    Namespace=self.namespace,
                    UserName=username,
                    SessionName=session_name,
                    Tags=[
                        {
                            'Key': 'EmbeddingUser',
                            'Value': 'True'
                        }
                    ]
                )
                user_arn = create_response['User']['Arn']
                print(f"Created new user: {user_arn}")
            
            return {
                'success': True,
                'user_arn': user_arn,
                'username': username
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def set_dashboard_permissions(self, dashboard_id, user_arn, permissions=None):
        """Set dashboard permissions for embedded user"""
        
        if not permissions:
            permissions = [
                'quicksight:DescribeDashboard',
                'quicksight:ListDashboardVersions',
                'quicksight:QueryDashboard'
            ]
        
        try:
            response = self.quicksight.update_dashboard_permissions(
                AwsAccountId=self.account_id,
                DashboardId=dashboard_id,
                GrantPermissions=[
                    {
                        'Principal': user_arn,
                        'Actions': permissions
                    }
                ]
            )
            
            return {
                'success': True,
                'permissions_set': permissions
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

# Initialize embedding service
embedding_service = QuickSightEmbedding(AWS_ACCOUNT_ID, QUICKSIGHT_NAMESPACE)

# Authentication decorator
def require_auth(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return jsonify({'error': 'Authorization header required'}), 401
        
        # Add your authentication logic here
        # This could integrate with your existing auth system
        
        return f(*args, **kwargs)
    return decorated_function

@app.route('/api/quicksight/embed-url', methods=['POST'])
@require_auth
def get_embed_url():
    """API endpoint to generate embedding URL"""
    
    data = request.json
    
    # Extract parameters
    dashboard_id = data.get('dashboard_id')
    user_email = data.get('user_email')
    username = data.get('username', user_email.split('@')[0])
    user_role = data.get('user_role', 'READER')
    
    # Validate required parameters
    if not dashboard_id or not user_email:
        return jsonify({
            'error': 'dashboard_id and user_email are required'
        }), 400
    
    try:
        # Create or get user
        user_result = embedding_service.create_or_update_user(
            username=username,
            email=user_email, 
            role=user_role
        )
        
        if not user_result['success']:
            return jsonify({
                'error': f"Failed to create/get user: {user_result['error']}"
            }), 500
        
        # Set dashboard permissions
        perm_result = embedding_service.set_dashboard_permissions(
            dashboard_id=dashboard_id,
            user_arn=user_result['user_arn']
        )
        
        if not perm_result['success']:
            print(f"Warning: Failed to set permissions: {perm_result['error']}")
        
        # Generate embed URL
        embed_result = embedding_service.get_dashboard_embed_url(
            dashboard_id=dashboard_id,
            user_arn=user_result['user_arn']
        )
        
        if embed_result['success']:
            return jsonify({
                'embed_url': embed_result['embed_url'],
                'user_arn': user_result['user_arn'],
                'expires_in': 600  # 10 minutes
            })
        else:
            return jsonify({
                'error': f"Failed to generate embed URL: {embed_result['error']}"
            }), 500
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/quicksight/anonymous-embed-url', methods=['POST'])
def get_anonymous_embed_url():
    """API endpoint for anonymous dashboard embedding"""
    
    data = request.json
    dashboard_id = data.get('dashboard_id')
    allowed_domains = data.get('allowed_domains', ['https://yourdomain.com'])
    
    if not dashboard_id:
        return jsonify({'error': 'dashboard_id is required'}), 400
    
    try:
        embed_result = embedding_service.get_anonymous_embed_url(
            dashboard_id=dashboard_id,
            allowed_domains=allowed_domains
        )
        
        if embed_result['success']:
            return jsonify({
                'embed_url': embed_result['embed_url'],
                'expires_in': 600
            })
        else:
            return jsonify({
                'error': f"Failed to generate anonymous embed URL: {embed_result['error']}"
            }), 500
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/quicksight/session-info', methods=['GET'])
@require_auth
def get_session_info():
    """Get current QuickSight session information"""
    
    try:
        # This would typically integrate with your session management
        session_info = {
            'session_id': 'embedded-session-123',
            'user': 'john.doe@company.com',
            'active_dashboards': ['sales-dashboard', 'operations-dashboard'],
            'session_start': datetime.now().isoformat(),
            'expires_at': (datetime.now() + timedelta(hours=10)).isoformat()
        }
        
        return jsonify(session_info)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Health check endpoint
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'service': 'quicksight-embedding-api'
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>QuickSight Embedded Dashboard</title>
    
    <!-- QuickSight Embedding SDK -->
    <script src="https://unpkg.com/amazon-quicksight-embedding-sdk@1.19.0/dist/quicksight-embedding-js-sdk.min.js"></script>
    
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        
        .dashboard-container {
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            padding: 20px;
            margin-bottom: 20px;
        }
        
        .dashboard-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            padding-bottom: 15px;
            border-bottom: 1px solid #eee;
        }
        
        .dashboard-controls {
            display: flex;
            gap: 10px;
        }
        
        .btn {
            background: #ff9900;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
        }
        
        .btn:hover {
            background: #e68900;
        }
        
        .btn:disabled {
            background: #ccc;
            cursor: not-allowed;
        }
        
        #quicksight-container {
            width: 100%;
            height: 800px;
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        
        .loading-spinner {
            display: flex;
            justify-content: center;
            align-items: center;
            height: 800px;
            font-size: 18px;
            color: #666;
        }
        
        .error-message {
            background: #f8d7da;
            color: #721c24;
            padding: 15px;
            border-radius: 4px;
            margin: 20px 0;
        }
        
        .success-message {
            background: #d4edda;
            color: #155724;
            padding: 15px;
            border-radius: 4px;
            margin: 20px 0;
        }
        
        @media (max-width: 768px) {
            .dashboard-header {
                flex-direction: column;
                align-items: flex-start;
            }
            
            .dashboard-controls {
                margin-top: 10px;
                flex-wrap: wrap;
            }
            
            #quicksight-container {
                height: 600px;
            }
        }
    </style>
</head>
<body>
    <div class="dashboard-container">
        <div class="dashboard-header">
            <h1 id="dashboard-title">Sales Performance Dashboard</h1>
            <div class="dashboard-controls">
                <button class="btn" onclick="refreshDashboard()">Refresh</button>
                <button class="btn" onclick="exportToPDF()">Export PDF</button>
                <button class="btn" onclick="fullscreen()">Fullscreen</button>
                <button class="btn" onclick="resetFilters()">Reset Filters</button>
            </div>
        </div>
        
        <div id="loading-indicator" class="loading-spinner">
            Loading dashboard...
        </div>
        
        <div id="error-container"></div>
        <div id="success-container"></div>
        
        <div id="quicksight-container" style="display: none;"></div>
    </div>

    <script>
        class QuickSightEmbedding {
            constructor() {
                this.dashboard = null;
                this.isLoaded = false;
                this.currentUser = null;
                this.sessionInfo = null;
            }
            
            async initialize() {
                try {
                    // Get user information from your auth system
                    this.currentUser = await this.getCurrentUser();
                    
                    // Load dashboard
                    await this.loadDashboard('sales-performance-dashboard');
                    
                } catch (error) {
                    this.showError(`Initialization failed: ${error.message}`);
                }
            }
            
            async getCurrentUser() {
                // This would integrate with your authentication system
                return {
                    email: 'john.doe@company.com',
                    username: 'john.doe',
                    role: 'READER',
                    department: 'Sales'
                };
            }
            
            async getEmbedUrl(dashboardId) {
                try {
                    const response = await fetch('/api/quicksight/embed-url', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Authorization': `Bearer ${this.getAuthToken()}`
                        },
                        body: JSON.stringify({
                            dashboard_id: dashboardId,
                            user_email: this.currentUser.email,
                            username: this.currentUser.username,
                            user_role: this.currentUser.role
                        })
                    });
                    
                    if (!response.ok) {
                        throw new Error(`HTTP error! status: ${response.status}`);
                    }
                    
                    const data = await response.json();
                    return data.embed_url;
                    
                } catch (error) {
                    throw new Error(`Failed to get embed URL: ${error.message}`);
                }
            }
            
            async loadDashboard(dashboardId, filters = {}) {
                try {
                    this.showLoading(true);
                    
                    // Get embed URL from backend
                    const embedUrl = await this.getEmbedUrl(dashboardId);
                    
                    // Configure embedding options
                    const options = {
                        url: embedUrl,
                        container: document.getElementById('quicksight-container'),
                        parameters: this.buildParameters(filters),
                        scrolling: 'auto',
                        height: '800px',
                        width: '100%',
                        locale: 'en-US',
                        footerPaddingEnabled: true,
                        printEnabled: true,
                        sheetTabsDisabled: false,
                        loadCallback: this.onDashboardLoad.bind(this),
                        errorCallback: this.onDashboardError.bind(this)
                    };
                    
                    // Load dashboard using QuickSight SDK
                    this.dashboard = QuickSightEmbedding.embedDashboard(options);
                    
                } catch (error) {
                    this.showError(`Failed to load dashboard: ${error.message}`);
                    this.showLoading(false);
                }
            }
            
            buildParameters(filters) {
                // Convert filters to QuickSight parameters format
                const parameters = {};
                
                for (const [key, value] of Object.entries(filters)) {
                    parameters[key] = {
                        Name: key,
                        Values: Array.isArray(value) ? value : [value]
                    };
                }
                
                return parameters;
            }
            
            onDashboardLoad(payload) {
                console.log('Dashboard loaded successfully', payload);
                this.isLoaded = true;
                this.showLoading(false);
                this.showSuccess('Dashboard loaded successfully');
                
                document.getElementById('quicksight-container').style.display = 'block';
                
                // Set up event listeners
                this.setupEventListeners();
            }
            
            onDashboardError(payload) {
                console.error('Dashboard load error:', payload);
                this.showError(`Dashboard failed to load: ${payload.errorMessage}`);
                this.showLoading(false);
            }
            
            setupEventListeners() {
                if (!this.dashboard) return;
                
                // Listen for parameter changes
                this.dashboard.on('parametersChanged', (payload) => {
                    console.log('Parameters changed:', payload);
                    this.onParametersChanged(payload);
                });
                
                // Listen for filter changes
                this.dashboard.on('filtersChanged', (payload) => {
                    console.log('Filters changed:', payload);
                    this.onFiltersChanged(payload);
                });
                
                // Listen for navigation events
                this.dashboard.on('navigate', (payload) => {
                    console.log('Navigation event:', payload);
                });
                
                // Listen for resize events
                window.addEventListener('resize', () => {
                    if (this.dashboard) {
                        this.dashboard.setFrameSize();
                    }
                });
            }
            
            onParametersChanged(payload) {
                // Handle parameter changes
                const parameters = payload.changedParameters;
                console.log('Dashboard parameters updated:', parameters);
                
                // You could save state or trigger other actions here
            }
            
            onFiltersChanged(payload) {
                // Handle filter changes
                const filters = payload.appliedFilters;
                console.log('Dashboard filters updated:', filters);
            }
            
            async refreshDashboard() {
                if (!this.dashboard) return;
                
                try {
                    await this.dashboard.setParameters({});
                    this.showSuccess('Dashboard refreshed');
                } catch (error) {
                    this.showError(`Refresh failed: ${error.message}`);
                }
            }
            
            async exportToPDF() {
                if (!this.dashboard) return;
                
                try {
                    await this.dashboard.printToPDF();
                    this.showSuccess('PDF export initiated');
                } catch (error) {
                    this.showError(`Export failed: ${error.message}`);
                }
            }
            
            async resetFilters() {
                if (!this.dashboard) return;
                
                try {
                    await this.dashboard.reset();
                    this.showSuccess('Filters reset');
                } catch (error) {
                    this.showError(`Reset failed: ${error.message}`);
                }
            }
            
            fullscreen() {
                const container = document.getElementById('quicksight-container');
                if (container.requestFullscreen) {
                    container.requestFullscreen();
                } else if (container.webkitRequestFullscreen) {
                    container.webkitRequestFullscreen();
                } else if (container.msRequestFullscreen) {
                    container.msRequestFullscreen();
                }
            }
            
            showLoading(show) {
                const loadingIndicator = document.getElementById('loading-indicator');
                loadingIndicator.style.display = show ? 'flex' : 'none';
            }
            
            showError(message) {
                const errorContainer = document.getElementById('error-container');
                errorContainer.innerHTML = `<div class="error-message">${message}</div>`;
                setTimeout(() => {
                    errorContainer.innerHTML = '';
                }, 5000);
            }
            
            showSuccess(message) {
                const successContainer = document.getElementById('success-container');
                successContainer.innerHTML = `<div class="success-message">${message}</div>`;
                setTimeout(() => {
                    successContainer.innerHTML = '';
                }, 3000);
            }
            
            getAuthToken() {
                // Return your auth token
                return localStorage.getItem('auth_token') || 'demo-token';
            }
        }
        
        // Global functions for button clicks
        let embedManager;
        
        function refreshDashboard() {
            if (embedManager) {
                embedManager.refreshDashboard();
            }
        }
        
        function exportToPDF() {
            if (embedManager) {
                embedManager.exportToPDF();
            }
        }
        
        function fullscreen() {
            if (embedManager) {
                embedManager.fullscreen();
            }
        }
        
        function resetFilters() {
            if (embedManager) {
                embedManager.resetFilters();
            }
        }
        
        // Initialize when page loads
        document.addEventListener('DOMContentLoaded', async function() {
            embedManager = new QuickSightEmbedding();
            await embedManager.initialize();
        });
        
        // Handle authentication state changes
        window.addEventListener('storage', function(e) {
            if (e.key === 'auth_token' && !e.newValue) {
                // User logged out, redirect or reload
                window.location.href = '/login';
            }
        });
    </script>
</body>
</html>
        ''', language='html')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
import boto3
import json
from datetime import datetime, timedelta

# Authentication setup for QuickSight embedding
class QuickSightAuthentication:
    """Handle authentication for QuickSight embedding"""
    
    def __init__(self, account_id, region='us-east-1'):
        self.account_id = account_id
        self.region = region
        self.sts_client = boto3.client('sts', region_name=region)
        self.quicksight = boto3.client('quicksight', region_name=region)
    
    def setup_saml_sso_integration(self):
        """Set up SAML SSO integration for QuickSight"""
        
        # SAML Identity Provider configuration
        saml_config = {
            "identity_provider": {
                "type": "SAML_2_0",
                "metadata_url": "https://your-idp.com/saml/metadata",
                "sso_url": "https://your-idp.com/saml/sso",
                "issuer": "https://your-idp.com",
                "certificate": "-----BEGIN CERTIFICATE-----\nMIIC...\n-----END CERTIFICATE-----"
            },
            "attribute_mapping": {
                "email": "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress",
                "role": "http://schemas.microsoft.com/ws/2008/06/identity/claims/role",
                "groups": "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/groups"
            }
        }
        
        # Create IAM Identity Provider
        iam = boto3.client('iam')
        
        try:
            # Create SAML identity provider
            idp_response = iam.create_saml_provider(
                SAMLMetadataDocument=self.get_saml_metadata(),
                Name='QuickSight-SAML-Provider',
                Tags=[
                    {'Key': 'Purpose', 'Value': 'QuickSight-Embedding'},
                    {'Key': 'Environment', 'Value': 'Production'}
                ]
            )
            
            print(f"SAML Provider created: {idp_response['SAMLProviderArn']}")
            
            # Create trust policy for QuickSight role
            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Federated": idp_response['SAMLProviderArn']
                        },
                        "Action": "sts:AssumeRoleWithSAML",
                        "Condition": {
                            "StringEquals": {
                                "SAML:aud": "https://signin.aws.amazon.com/saml"
                            }
                        }
                    }
                ]
            }
            
            # Create role for SAML users
            role_response = iam.create_role(
                RoleName='QuickSight-SAML-Role',
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description='Role for SAML-authenticated QuickSight users',
                MaxSessionDuration=43200,  # 12 hours
                Tags=[
                    {'Key': 'Service', 'Value': 'QuickSight'},
                    {'Key': 'AuthMethod', 'Value': 'SAML'}
                ]
            )
            
            # Attach QuickSight policy
            iam.attach_role_policy(
                RoleName='QuickSight-SAML-Role',
                PolicyArn='arn:aws:iam::aws:policy/service-role/AWSQuickSightEmbeddingRole'
            )
            
            return {
                'saml_provider_arn': idp_response['SAMLProviderArn'],
                'role_arn': role_response['Role']['Arn'],
                'config': saml_config
            }
            
        except Exception as e:
            print(f"Error setting up SAML SSO: {e}")
            return None
    
    def setup_cognito_integration(self):
        """Set up Amazon Cognito integration"""
        
        cognito_idp = boto3.client('cognito-idp')
        
        try:
            # Create Cognito User Pool
            user_pool_response = cognito_idp.create_user_pool(
                PoolName='QuickSight-Users',
                Policies={
                    'PasswordPolicy': {
                        'MinimumLength': 8,
                        'RequireUppercase': True,
                        'RequireLowercase': True,
                        'RequireNumbers': True,
                        'RequireSymbols': False
                    }
                },
                MfaConfiguration='OPTIONAL',  # Enable MFA
                AccountRecoverySetting={
                    'RecoveryMechanisms': [
                        {
                            'Priority': 1,
                            'Name': 'verified_email'
                        }
                    ]
                },
                Schema=[
                    {
                        'Name': 'email',
                        'AttributeDataType': 'String',
                        'Required': True,
                        'Mutable': True
                    },
                    {
                        'Name': 'department',
                        'AttributeDataType': 'String',
                        'Required': False,
                        'Mutable': True
                    },
                    {
                        'Name': 'role',
                        'AttributeDataType': 'String',
                        'Required': False,
                        'Mutable': True
                    }
                ],
                Tags={
                    'Service': 'QuickSight',
                    'Purpose': 'Embedding'
                }
            )
            
            user_pool_id = user_pool_response['UserPool']['Id']
            
            # Create User Pool Client
            client_response = cognito_idp.create_user_pool_client(
                UserPoolId=user_pool_id,
                ClientName='QuickSight-Embedding-Client',
                ExplicitAuthFlows=[
                    'ALLOW_USER_SRP_AUTH',
                    'ALLOW_REFRESH_TOKEN_AUTH'
                ],
                GenerateSecret=True,  # Use client secret
                TokenValidityUnits={
                    'AccessToken': 'hours',
                    'IdToken': 'hours',
                    'RefreshToken': 'days'
                },
                AccessTokenValidity=12,  # 12 hours
                IdTokenValidity=12,      # 12 hours
                RefreshTokenValidity=30  # 30 days
            )
            
            # Create Identity Pool
            cognito_identity = boto3.client('cognito-identity')
            
            identity_pool_response = cognito_identity.create_identity_pool(
                IdentityPoolName='QuickSight Embedding Pool',
                AllowUnauthenticatedIdentities=False,
                CognitoIdentityProviders=[
                    {
                        'ProviderName': f'cognito-idp.{self.region}.amazonaws.com/{user_pool_id}',
                        'ClientId': client_response['UserPoolClient']['ClientId'],
                        'ServerSideTokenCheck': True
                    }
                ],
                Tags={
                    'Service': 'QuickSight',
                    'Purpose': 'Embedding'
                }
            )
            
            return {
                'user_pool_id': user_pool_id,
                'client_id': client_response['UserPoolClient']['ClientId'],
                'client_secret': client_response['UserPoolClient']['ClientSecret'],
                'identity_pool_id': identity_pool_response['IdentityPoolId']
            }
            
        except Exception as e:
            print(f"Error setting up Cognito: {e}")
            return None
    
    def create_embedding_role_policy(self):
        """Create IAM policy for QuickSight embedding"""
        
        iam = boto3.client('iam')
        
        # Comprehensive policy for embedding
        embedding_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "quicksight:RegisterUser",
                        "quicksight:DescribeUser",
                        "quicksight:UpdateUser",
                        "quicksight:DeleteUser",
                        "quicksight:ListUsers"
                    ],
                    "Resource": f"arn:aws:quicksight:*:{self.account_id}:user/*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "quicksight:DescribeDashboard",
                        "quicksight:ListDashboardVersions",
                        "quicksight:QueryDashboard",
                        "quicksight:GetDashboardEmbedUrl",
                        "quicksight:GenerateEmbedUrlForAnonymousUser",
                        "quicksight:GenerateEmbedUrlForRegisteredUser"
                    ],
                    "Resource": f"arn:aws:quicksight:*:{self.account_id}:dashboard/*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "quicksight:DescribeDataSet",
                        "quicksight:DescribeDataSource",
                        "quicksight:PassDataSet",
                        "quicksight:PassDataSource"
                    ],
                    "Resource": [
                        f"arn:aws:quicksight:*:{self.account_id}:dataset/*",
                        f"arn:aws:quicksight:*:{self.account_id}:datasource/*"
                    ]
                },
                {
                    "Effect": "Allow", 
                    "Action": [
                        "quicksight:CreateGroup",
                        "quicksight:DescribeGroup",
                        "quicksight:UpdateGroup",
                        "quicksight:DeleteGroup",
                        "quicksight:ListGroups",
                        "quicksight:CreateGroupMembership",
                        "quicksight:DeleteGroupMembership",
                        "quicksight:ListGroupMemberships"
                    ],
                    "Resource": f"arn:aws:quicksight:*:{self.account_id}:group/*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "quicksight:ListDashboards",
                        "quicksight:ListAnalyses",
                        "quicksight:ListDataSets",
                        "quicksight:ListDataSources"
                    ],
                    "Resource": "*"
                }
            ]
        }
        
        try:
            # Create policy
            policy_response = iam.create_policy(
                PolicyName='QuickSightEmbeddingPolicy',
                PolicyDocument=json.dumps(embedding_policy),
                Description='Policy for QuickSight dashboard embedding'
            )
            
            # Create role with trust policy
            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "lambda.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    },
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "AWS": f"arn:aws:iam::{self.account_id}:root"
                        },
                        "Action": "sts:AssumeRole",
                        "Condition": {
                            "StringEquals": {
                                "sts:ExternalId": "QuickSightEmbedding"
                            }
                        }
                    }
                ]
            }
            
            role_response = iam.create_role(
                RoleName='QuickSightEmbeddingRole',
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description='IAM role for QuickSight embedding operations'
            )
            
            # Attach policy to role
            iam.attach_role_policy(
                RoleName='QuickSightEmbeddingRole',
                PolicyArn=policy_response['Policy']['Arn']
            )
            
            return {
                'policy_arn': policy_response['Policy']['Arn'],
                'role_arn': role_response['Role']['Arn']
            }
            
        except Exception as e:
            print(f"Error creating embedding policy: {e}")
            return None
    
    def get_temporary_credentials(self, role_arn, session_name, external_id=None):
        """Get temporary credentials for embedding operations"""
        
        try:
            assume_role_params = {
                'RoleArn': role_arn,
                'RoleSessionName': session_name,
                'DurationSeconds': 3600  # 1 hour
            }
            
            if external_id:
                assume_role_params['ExternalId'] = external_id
            
            response = self.sts_client.assume_role(**assume_role_params)
            
            credentials = response['Credentials']
            
            return {
                'access_key': credentials['AccessKeyId'],
                'secret_key': credentials['SecretAccessKey'],
                'session_token': credentials['SessionToken'],
                'expires_at': credentials['Expiration']
            }
            
        except Exception as e:
            print(f"Error getting temporary credentials: {e}")
            return None
    
    def get_saml_metadata(self):
        """Get SAML metadata document"""
        # This would typically be fetched from your identity provider
        return """<?xml version="1.0" encoding="UTF-8"?>
        <md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata" 
                            entityID="https://your-idp.com">
            <md:IDPSSODescriptor WantAuthnRequestsSigned="false" 
                               protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">
                <md:KeyDescriptor use="signing">
                    <ds:KeyInfo xmlns:ds="http://www.w3.org/2000/09/xmldsig#">
                        <ds:X509Data>
                            <ds:X509Certificate>
                                MIICertificateDataHere
                            </ds:X509Certificate>
                        </ds:X509Data>
                    </ds:KeyInfo>
                </md:KeyDescriptor>
                <md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"
                                      Location="https://your-idp.com/saml/sso"/>
            </md:IDPSSODescriptor>
        </md:EntityDescriptor>"""

# Complete authentication setup
def setup_complete_authentication():
    """Set up complete authentication infrastructure"""
    
    account_id = '123456789012'
    auth_manager = QuickSightAuthentication(account_id)
    
    print("🚀 Setting up QuickSight authentication...")
    
    # 1. Create embedding policy and role
    print("Creating embedding policy and role...")
    policy_result = auth_manager.create_embedding_role_policy()
    
    if policy_result:
        print(f"✅ Policy created: {policy_result['policy_arn']}")
        print(f"✅ Role created: {policy_result['role_arn']}")
    
    # 2. Set up SAML SSO (optional)
    print("\nSetting up SAML SSO...")
    saml_result = auth_manager.setup_saml_sso_integration()
    
    if saml_result:
        print(f"✅ SAML Provider: {saml_result['saml_provider_arn']}")
        print(f"✅ SAML Role: {saml_result['role_arn']}")
    
    # 3. Set up Cognito (optional)
    print("\nSetting up Cognito integration...")
    cognito_result = auth_manager.setup_cognito_integration()
    
    if cognito_result:
        print(f"✅ User Pool: {cognito_result['user_pool_id']}")
        print(f"✅ Client ID: {cognito_result['client_id']}")
    
    print("\n✅ Authentication setup complete!")
    
    return {
        'policy_result': policy_result,
        'saml_result': saml_result,
        'cognito_result': cognito_result
    }

if __name__ == "__main__":
    setup_complete_authentication()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

# Helper functions
def generate_insights(data, metric, dimension):
    """Generate ML-like insights from data"""
    if data.empty:
        return {
            'key_finding': 'No data available',
            'trend': 'Cannot determine',
            'anomaly': 'None detected',
            'recommendation': 'Check data sources'
        }
    
    # Simple analytics to simulate insights
    grouped = data.groupby(dimension)[metric].sum().sort_values(ascending=False)
    top_performer = grouped.index[0] if len(grouped) > 0 else 'Unknown'
    
    return {
        'key_finding': f'{top_performer} is the top performer in {dimension}',
        'trend': f'{metric} showing upward trend in recent period',
        'anomaly': 'Unusual spike detected on weekends',
        'recommendation': f'Focus marketing efforts on {top_performer} segment'
    }

def calculate_spice_metrics(size_gb, num_datasets, frequency, compression, users, complexity):
    """Calculate SPICE capacity requirements and estimates"""
    
    # Apply compression
    effective_size = size_gb * num_datasets * (1 - compression)
    
    # Add buffer for calculations and intermediate results
    complexity_multiplier = {'Simple': 1.2, 'Medium': 1.5, 'Complex': 2.0}
    buffer_multiplier = complexity_multiplier.get(complexity, 1.5)
    
    total_capacity = effective_size * buffer_multiplier
    
    # Calculate costs (simplified model)
    spice_cost_per_gb = 0.25  # per GB per month
    monthly_cost = total_capacity * spice_cost_per_gb
    
    # Performance gain estimate
    complexity_performance = {'Simple': 8, 'Medium': 12, 'Complex': 20}
    base_performance = complexity_performance.get(complexity, 12)
    
    return total_capacity, monthly_cost, base_performance

def create_data_sources_matrix():
    """Create comprehensive data sources compatibility matrix"""
    data = {
        'Category': ['AWS', 'AWS', 'AWS', 'AWS', 'AWS', 'AWS', 'External', 'External', 'External', 'External'],
        'Source': ['Amazon S3', 'Amazon Redshift', 'Amazon RDS', 'Amazon Athena', 'Amazon Aurora', 'Amazon EMR', 
                  'Salesforce', 'Oracle', 'SQL Server', 'MySQL'],
        'SPICE Support': ['Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes'],
        'Real-time': ['No', 'Limited', 'Yes', 'No', 'Yes', 'No', 'Limited', 'Yes', 'Yes', 'Yes'],
        'Connection Type': ['File', 'JDBC', 'JDBC', 'API', 'JDBC', 'JDBC', 'API', 'JDBC', 'JDBC', 'JDBC']
    }
    return pd.DataFrame(data)

def calculate_embedding_config(app_type, auth_method, user_capacity, dashboard_type, features, branding):
    """Calculate embedding configuration recommendations"""
    
    # Base complexity scoring
    complexity_scores = {
        'app_type': {'Web Application': 2, 'Mobile App': 3, 'Portal/Intranet': 2, 'SaaS Platform': 4},
        'auth_method': {'SAML SSO': 4, 'Active Directory': 4, 'Amazon Cognito': 2, 'Custom OAuth': 3},
        'user_capacity': {'< 100': 1, '100-1,000': 2, '1,000-10,000': 3, '> 10,000': 4},
        'dashboard_type': {'Executive Summary': 2, 'Operational Metrics': 3, 'Customer Analytics': 4, 'Financial Reports': 3}
    }
    
    total_complexity = (
        complexity_scores['app_type'].get(app_type, 2) +
        complexity_scores['auth_method'].get(auth_method, 2) +
        complexity_scores['user_capacity'].get(user_capacity, 2) +
        complexity_scores['dashboard_type'].get(dashboard_type, 2)
    )
    
    # Determine architecture pattern
    if total_complexity <= 8:
        pattern = "Simple Embedding"
        effort = 5
    elif total_complexity <= 12:
        pattern = "Enterprise Embedding"
        effort = 15
    else:
        pattern = "Advanced Multi-tenant"
        effort = 30
    
    # Cost estimation (simplified)
    user_multipliers = {'< 100': 500, '100-1,000': 2000, '1,000-10,000': 8000, '> 10,000': 20000}
    base_cost = user_multipliers.get(user_capacity, 2000) * 0.15  # Assuming 15% average usage
    
    return {
        'pattern': pattern,
        'cost_estimate': int(base_cost),
        'effort': effort,
        'auth_complexity': complexity_scores['auth_method'].get(auth_method, 2),
        'scaling': f"Auto-scaling for {user_capacity} users"
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
    # 📊 Amazon QuickSight Analytics Platform
    <div class='info-box'>
    Master Amazon QuickSight's powerful business intelligence capabilities, from SPICE engine optimization to embedded analytics integration for modern applications.
    </div>
    """, unsafe_allow_html=True)
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs([
        "📊 QuickSight",
        "💾 Accessing data with QuickSight", 
        "🔗 QuickSight embedded analytics"
    ])
    
    with tab1:
        quicksight_tab()
    
    with tab2:
        accessing_data_quicksight_tab()
    
    with tab3:
        quicksight_embedded_analytics_tab()
    
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
