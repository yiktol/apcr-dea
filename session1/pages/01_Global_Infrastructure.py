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
    page_title="AWS Global Infrastructure",
    page_icon="☁️",
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
    'success': '#3FB34F'
}

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
    </style>
    """, unsafe_allow_html=True)

def initialize_session_state():
    """Initialize session state variables"""
    
    common.initialize_session_state()
    if 'session_started' not in st.session_state:
        st.session_state.session_started = True
        st.session_state.regions_explored = []
        st.session_state.concepts_learned = []

def create_sidebar():
    """Create sidebar with app information and controls"""
    with st.sidebar:
        common.render_sidebar()
        
        # About section (collapsed by default)
        with st.expander("📖 About this App", expanded=False):
            st.markdown("""
            **Topics Covered:**
            - 🌍 AWS Regions - Physical locations worldwide
            - 🏢 Availability Zones - Isolated data centers
            - 📡 Edge Locations - Content delivery network
            
            **Learning Objectives:**
            - Understand AWS global infrastructure components
            - Learn about high availability and fault tolerance
            - Explore real-world examples and use cases
            """)


def create_regions_mermaid():
    """Create mermaid diagram for AWS Regions"""
    return """
    graph TD
        A[AWS Global Infrastructure] --> B[AWS Regions]
        B --> C[US East N. Virginia]
        B --> D[US West Oregon]
        B --> E[EU Ireland]
        B --> F[Asia Pacific Singapore]
        B --> G[South America São Paulo]
        
        C --> C1[Multiple AZs]
        D --> D1[Multiple AZs]
        E --> E1[Multiple AZs]
        F --> F1[Multiple AZs]
        G --> G1[Multiple AZs]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C fill:#3FB34F,stroke:#232F3E,color:#fff
        style D fill:#3FB34F,stroke:#232F3E,color:#fff
        style E fill:#3FB34F,stroke:#232F3E,color:#fff
        style F fill:#3FB34F,stroke:#232F3E,color:#fff
        style G fill:#3FB34F,stroke:#232F3E,color:#fff
    """

def create_az_mermaid():
    """Create mermaid diagram for Availability Zones"""
    return """
    graph TB
        subgraph "AWS Region (us-east-1)"
            AZ1[Availability Zone 1<br/>us-east-1a<br/>🏢 Data Center A<br/>🏢 Data Center B]
            AZ2[Availability Zone 2<br/>us-east-1b<br/>🏢 Data Center C<br/>🏢 Data Center D]
            AZ3[Availability Zone 3<br/>us-east-1c<br/>🏢 Data Center E<br/>🏢 Data Center F]
        end
        
        AZ1 -.->|Low Latency<br/>Fiber Network| AZ2
        AZ2 -.->|Low Latency<br/>Fiber Network| AZ3
        AZ3 -.->|Low Latency<br/>Fiber Network| AZ1
        
        style AZ1 fill:#FF9900,stroke:#232F3E,color:#fff
        style AZ2 fill:#FF9900,stroke:#232F3E,color:#fff
        style AZ3 fill:#FF9900,stroke:#232F3E,color:#fff
    """

def create_edge_locations_mermaid():
    """Create mermaid diagram for Edge Locations"""
    return """
    graph TD
        User1[👤 User in New York] --> Edge1[📡 Edge Location<br/>New York]
        User2[👤 User in London] --> Edge2[📡 Edge Location<br/>London]
        User3[👤 User in Tokyo] --> Edge3[📡 Edge Location<br/>Tokyo]
        
        Edge1 --> CF[☁️ CloudFront CDN]
        Edge2 --> CF
        Edge3 --> CF
        
        CF --> Origin[🗄️ Origin Server<br/>S3 Bucket<br/>US East]
        
        style User1 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style User2 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style User3 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style Edge1 fill:#FF9900,stroke:#232F3E,color:#fff
        style Edge2 fill:#FF9900,stroke:#232F3E,color:#fff
        style Edge3 fill:#FF9900,stroke:#232F3E,color:#fff
        style CF fill:#3FB34F,stroke:#232F3E,color:#fff
        style Origin fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_latency_chart():
    """Create interactive latency comparison chart"""
    data = {
        'Scenario': ['Without Edge Locations', 'With Edge Locations'],
        'New York to Virginia': [50, 5],
        'London to Virginia': [150, 10],
        'Tokyo to Virginia': [180, 8],
        'Sydney to Virginia': [200, 12]
    }
    
    df = pd.DataFrame(data)
    df_melted = pd.melt(df, id_vars=['Scenario'], var_name='Route', value_name='Latency (ms)')
    
    fig = px.bar(df_melted, x='Route', y='Latency (ms)', color='Scenario',
                 title='Latency Comparison: With vs Without Edge Locations',
                 color_discrete_sequence=[AWS_COLORS['secondary'], AWS_COLORS['primary']])
    
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font_color=AWS_COLORS['secondary']
    )
    
    return fig

def create_availability_simulation():
    """Create availability zones failover simulation"""
    col1, col2, col3 = st.columns(3)
    
    with col1:
        az1_status = st.selectbox("AZ-1a Status", ["🟢 Healthy", "🔴 Failed"], key="az1")
    with col2:
        az2_status = st.selectbox("AZ-1b Status", ["🟢 Healthy", "🔴 Failed"], key="az2")
    with col3:
        az3_status = st.selectbox("AZ-1c Status", ["🟢 Healthy", "🔴 Failed"], key="az3")
    
    healthy_azs = sum([1 for status in [az1_status, az2_status, az3_status] if "Healthy" in status])
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    if healthy_azs >= 2:
        st.markdown("### ✅ System Status: OPERATIONAL")
        st.markdown(f"**{healthy_azs}/3 AZs are healthy** - Your application continues to run with high availability!")
    elif healthy_azs == 1:
        st.markdown("### ⚠️ System Status: DEGRADED")
        st.markdown("**Only 1/3 AZs healthy** - Consider immediate action to restore redundancy.")
    else:
        st.markdown("### 🔴 System Status: CRITICAL")
        st.markdown("**All AZs failed** - Complete service outage. This scenario is extremely rare.")
    st.markdown('</div>', unsafe_allow_html=True)

def aws_regions_tab():
    """Content for AWS Regions tab"""
    st.markdown("# 🌍 AWS Regions")
    st.markdown("*Physical locations around the world where AWS clusters data centers*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    An **AWS Region** is a physical location around the world where AWS clusters data centers. 
    Each Region consists of multiple, isolated, and physically separate Availability Zones within a geographic area.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive Region Map
    st.markdown("## 🗺️ Global Region Distribution")
    
    # Sample region data
    # regions_data = {
    #     'Region': ['us-east-1', 'us-west-2', 'eu-west-1', 'ap-southeast-1', 'sa-east-1'],
    #     'Location': ['N. Virginia', 'Oregon', 'Ireland', 'Singapore', 'São Paulo'],
    #     'Lat': [38.9, 43.8, 53.3, 1.3, -23.5],
    #     'Lon': [-77.0, -120.5, -6.2, 103.8, -46.6],
    #     'AZ_Count': [6, 4, 3, 3, 3]
    # }


    regions_data = {
    'Region': [
        'us-east-1', 'us-east-2', 'us-west-1', 'us-west-2',
        'ca-central-1', 'ca-west-1',
        'eu-north-1', 'eu-west-1', 'eu-west-2', 'eu-west-3', 'eu-central-1', 'eu-central-2', 'eu-south-1', 'eu-south-2',
        'ap-northeast-1', 'ap-northeast-2', 'ap-northeast-3', 'ap-southeast-1', 'ap-southeast-2', 'ap-southeast-3', 'ap-southeast-4', 'ap-south-1', 'ap-south-2', 'ap-east-1',
        'sa-east-1',
        'af-south-1',
        'me-south-1', 'me-central-1',
        'il-central-1'
    ],
    'Location': [
        'N. Virginia', 'Ohio', 'N. California', 'Oregon',
        'Central Canada', 'Calgary',
        'Stockholm', 'Ireland', 'London', 'Paris', 'Frankfurt', 'Zurich', 'Milan', 'Spain',
        'Tokyo', 'Seoul', 'Osaka', 'Singapore', 'Sydney', 'Jakarta', 'Melbourne', 'Mumbai', 'Hyderabad', 'Hong Kong',
        'São Paulo',
        'Cape Town',
        'Bahrain', 'UAE',
        'Tel Aviv'
    ],
    'Lat': [
        38.9, 39.9, 37.4, 45.5,
        43.7, 51.0,
        59.3, 53.3, 51.5, 48.9, 50.1, 47.4, 45.5, 40.4,
        35.7, 37.6, 34.7, 1.3, -33.9, -6.2, -37.8, 19.1, 17.4, 22.3,
        -23.5,
        -33.9,
        26.2, 24.5,
        32.1
    ],
    'Lon': [
        -77.0, -82.9, -122.1, -121.3,
        -79.4, -114.1,
        18.1, -6.2, -0.1, 2.3, 8.7, 8.5, 9.2, -3.7,
        139.7, 126.9, 135.5, 103.8, 151.2, 106.8, 144.9, 72.9, 78.5, 114.2,
        -46.6,
        18.4,
        50.6, 54.4,
        34.8
    ],
    'AZ_Count': [
        6, 3, 3, 4,
        3, 3,
        3, 3, 3, 3, 3, 3, 3, 3,
        4, 4, 3, 3, 3, 3, 3, 3, 3, 3,
        3,
        3,
        3, 3,
        3
    ]
}

    
    df_regions = pd.DataFrame(regions_data)
    
    fig = px.scatter_mapbox(df_regions, lat="Lat", lon="Lon", hover_name="Region",
                           hover_data=["Location", "AZ_Count"], size="AZ_Count",
                           color_discrete_sequence=[AWS_COLORS['primary']],
                           zoom=1, height=400)
    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    st.plotly_chart(fig, use_container_width=True)
    
    # Mermaid diagram
    st.markdown("## 🏗️ AWS Region Architecture")
    common.mermaid(create_regions_mermaid(), height=300)
    
    # Interactive region explorer
    st.markdown("## 🔍 Region Explorer")
    selected_region = st.selectbox("Select a region to explore:", df_regions['Region'].tolist())
    
    if selected_region:
        region_info = df_regions[df_regions['Region'] == selected_region].iloc[0]
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(f'<div class="metric-card"><h3>{region_info["Region"]}</h3><p>Region Code</p></div>', unsafe_allow_html=True)
        with col2:
            st.markdown(f'<div class="metric-card"><h3>{region_info["Location"]}</h3><p>Location</p></div>', unsafe_allow_html=True)
        with col3:
            st.markdown(f'<div class="metric-card"><h3>{region_info["AZ_Count"]} AZs</h3><p>Availability Zones</p></div>', unsafe_allow_html=True)
    
    # Code example
    st.markdown("## 💻 Code Example: Specifying Regions")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
# AWS CLI - List all regions
aws ec2 describe-regions

# Python Boto3 - Create resource in specific region
import boto3

# Create S3 client in specific region
s3_client = boto3.client('s3', region_name='us-west-2')

# Create EC2 resource in specific region  
ec2_resource = boto3.resource('ec2', region_name='eu-west-1')

# List all available regions programmatically
ec2_client = boto3.client('ec2', region_name='us-east-1')
regions = ec2_client.describe_regions()
for region in regions['Regions']:
    print(f"Region: {region['RegionName']}")
    """, language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def aws_availability_zones_tab():
    """Content for AWS Availability Zones tab"""
    st.markdown("# 🏢 AWS Availability Zones (AZs)")
    st.markdown("*One or more discrete data centers with redundant power, networking, and connectivity*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **Availability Zones** are physically separate facilities within a region, typically tens of miles apart. 
    They provide isolation from disasters while maintaining low-latency connectivity through private fiber-optic networks.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # AZ Architecture diagram
    st.markdown("## 🏗️ Availability Zone Architecture")
    common.mermaid(create_az_mermaid())
    
    # Interactive AZ simulation
    st.markdown("## 🔄 High Availability Simulation")
    st.markdown("*Try failing different Availability Zones to see how AWS maintains service availability*")
    
    create_availability_simulation()
    
    # Benefits section
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ✅ Benefits of Multiple AZs
        - **High Availability**: Automatic failover between AZs
        - **Fault Tolerance**: Survive infrastructure failures  
        - **Disaster Recovery**: Protection from natural disasters
        - **Load Distribution**: Spread traffic across multiple zones
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔧 Design Principles
        - **Deploy across multiple AZs** for critical applications
        - **Minimum 100 miles separation** between AZs
        - **Low latency networking** (< 10ms between AZs)
        - **Independent infrastructure** per AZ
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Real-world example
    st.markdown("## 🌟 Real-World Example")
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown("""
    ### E-commerce Application Deployment
    
    **Scenario**: You're deploying a critical e-commerce application that must be available 24/7.
    
    **Multi-AZ Setup**:
    - **Web Servers**: Deployed in us-east-1a and us-east-1b
    - **Database**: RDS with Multi-AZ deployment (primary in 1a, standby in 1b)  
    - **Load Balancer**: Distributes traffic across both AZs
    
    **Result**: If us-east-1a experiences an outage, traffic automatically routes to us-east-1b with minimal disruption!
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Code example
    st.markdown("## 💻 Code Example: Multi-AZ Deployment")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
# Deploy EC2 instances across multiple AZs
import boto3

ec2 = boto3.resource('ec2', region_name='us-east-1')

# Launch instances in different AZs
instances = []

# Instance in AZ 1a
instance_1a = ec2.create_instances(
    ImageId='ami-0abcdef1234567890',
    MinCount=1,
    MaxCount=1,
    InstanceType='t3.micro',
    Placement={'AvailabilityZone': 'us-east-1a'}
)

# Instance in AZ 1b  
instance_1b = ec2.create_instances(
    ImageId='ami-0abcdef1234567890',
    MinCount=1,
    MaxCount=1, 
    InstanceType='t3.micro',
    Placement={'AvailabilityZone': 'us-east-1b'}
)

print("Instances deployed across multiple AZs for high availability!")
    """, language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def edge_locations_tab():
    """Content for Edge Locations tab"""
    st.markdown("# 📡 Edge Locations")
    st.markdown("*700+ CloudFront Points of Presence for global content delivery*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **Edge Locations** are smaller endpoints used for hosting cached data, located in major cities worldwide. 
    They form the backbone of AWS's Content Delivery Network (CDN) to reduce latency for end users.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Edge locations architecture
    st.markdown("## 🏗️ Edge Location Architecture")
    common.mermaid(create_edge_locations_mermaid(),height=350)
    
    # Interactive latency comparison
    st.markdown("## ⚡ Latency Impact Demonstration")
    st.plotly_chart(create_latency_chart(), use_container_width=True)
    
    # Services using edge locations
    st.markdown("## 🛠️ Services Using Edge Locations")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🚀 Primary Services
        - **Amazon CloudFront**: Global CDN service
        - **Amazon Route 53**: DNS web service  
        - **AWS WAF**: Web application firewall
        - **AWS Shield**: DDoS protection
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Key Statistics
        - **700+** Points of Presence globally
        - **13** Regional mid-tier cache servers
        - **Major cities** coverage worldwide
        - **Millisecond** response times
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive demo
    st.markdown("## 🎮 CloudFront Cache Demo")
    
    user_location = st.selectbox("Select your location:", 
                                ["New York, USA", "London, UK", "Tokyo, Japan", "Sydney, Australia"])
    
    cache_status = st.radio("Cache Status:", ["Cache MISS (First Request)", "Cache HIT (Subsequent Request)"])
    
    if cache_status == "Cache MISS (First Request)":
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 🔄 Cache MISS Scenario
        **User Location**: {user_location}  
        **Process**: Request → Edge Location → Origin Server (S3) → Edge Location → User  
        **Latency**: Higher (fetching from origin)  
        **Action**: Content cached at edge location for future requests
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ⚡ Cache HIT Scenario  
        **User Location**: {user_location}  
        **Process**: Request → Edge Location → User  
        **Latency**: Ultra-low (served from cache)  
        **Benefit**: 85-95% faster response time!
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Use cases
    st.markdown("## 🌟 Real-World Use Cases")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🎬 Video Streaming  
        **Netflix, YouTube**
        - Cache popular videos at edge locations
        - Reduce buffering and loading times
        - Handle millions of concurrent viewers
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🛒 E-commerce
        **Amazon, eBay**  
        - Cache product images and catalogs
        - Accelerate checkout processes
        - Improve user experience globally
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📱 Mobile Apps
        **Gaming, Social Media**
        - Cache API responses  
        - Reduce mobile data usage
        - Faster app loading times
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code example
    st.markdown("## 💻 Code Example: Setting up CloudFront")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
# Create CloudFront distribution using AWS CLI
aws cloudfront create-distribution --distribution-config file://distribution-config.json

# Python Boto3 - Create CloudFront distribution
import boto3

cloudfront = boto3.client('cloudfront')

response = cloudfront.create_distribution(
    DistributionConfig={
        'CallerReference': 'my-distribution-2024',
        'Comment': 'My CloudFront Distribution',
        'Origins': {
            'Quantity': 1,
            'Items': [
                {
                    'Id': 'my-s3-origin',
                    'DomainName': 'my-bucket.s3.amazonaws.com',
                    'S3OriginConfig': {
                        'OriginAccessIdentity': ''
                    }
                }
            ]
        },
        'DefaultCacheBehavior': {
            'TargetOriginId': 'my-s3-origin',
            'ViewerProtocolPolicy': 'redirect-to-https',
            'TrustedSigners': {
                'Enabled': False,
                'Quantity': 0
            }
        },
        'Enabled': True
    }
)

print(f"Distribution created: {response['Distribution']['DomainName']}")
    """, language='python')
    st.markdown('</div>', unsafe_allow_html=True)

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
    # ☁️ AWS Global Infrastructure
    ### Master the foundations of AWS's worldwide infrastructure
    """)
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs([
        "🌍 AWS Regions", 
        "🏢 Availability Zones", 
        "📡 Edge Locations"
    ])
    
    with tab1:
        aws_regions_tab()
    
    with tab2:
        aws_availability_zones_tab()
    
    with tab3:
        edge_locations_tab()
    
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


