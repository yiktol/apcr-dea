import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import utils.common as common
import utils.authenticate as authenticate
import time
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(
    page_title="AWS Scaling and Load Balancing Hub",
    page_icon="⚖️",
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
        
        .success-box {{
            background: linear-gradient(135deg, {AWS_COLORS['success']} 0%, {AWS_COLORS['light_blue']} 100%);
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
        
        .service-comparison {{
            background: white;
            padding: 20px;
            border-radius: 12px;
            border: 2px solid {AWS_COLORS['light_blue']};
            margin: 15px 0;
        }}
        
        .scaling-simulator {{
            background: white;
            padding: 20px;
            border-radius: 12px;
            border: 2px solid {AWS_COLORS['primary']};
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
            - 📈 Amazon EC2 Auto Scaling - Automatic capacity management
            - 👥 Auto Scaling Groups - Groups of EC2 instances
            - ⚖️ Elastic Load Balancing - Distribute traffic across instances
            
            **Learning Objectives:**
            - Understand auto scaling concepts and policies
            - Learn how to configure Auto Scaling Groups
            - Explore different load balancer types
            - Practice with interactive scaling simulators
            - Master cost optimization through scaling
            """)

def create_auto_scaling_architecture_mermaid():
    """Create mermaid diagram for Auto Scaling architecture"""
    return """
    graph TD
        A[CloudWatch Metrics] --> B[Auto Scaling Policies]
        B --> C[Auto Scaling Group]
        C --> D[Launch Template]
        C --> E[Target Group]
        
        D --> F[EC2 Instance 1]
        D --> G[EC2 Instance 2]
        D --> H[EC2 Instance N]
        
        E --> I[Application Load Balancer]
        I --> J[Internet Gateway]
        
        F --> K[Application]
        G --> L[Application]
        H --> M[Application]
        
        B --> N[Scale Out Policy]
        B --> O[Scale In Policy]
        
        N --> P[CPU > 70%]
        O --> Q[CPU < 30%]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C fill:#3FB34F,stroke:#232F3E,color:#fff
        style I fill:#FF6B35,stroke:#232F3E,color:#fff
    """

def create_asg_lifecycle_mermaid():
    """Create mermaid diagram for ASG lifecycle"""
    return """
    graph LR
        A[Launch Template] --> B[Instance Launch]
        B --> C[Health Check]
        C --> D{Healthy?}
        D -->|Yes| E[In Service]
        D -->|No| F[Terminate]
        
        E --> G[CloudWatch Monitoring]
        G --> H{Scaling Trigger?}
        H -->|Scale Out| I[Launch New Instance]
        H -->|Scale In| J[Terminate Instance]
        H -->|No Action| G
        
        I --> B
        J --> K[Graceful Shutdown]
        K --> L[Instance Terminated]
        
        F --> M[Launch Replacement]
        M --> B
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style E fill:#3FB34F,stroke:#232F3E,color:#fff
        style G fill:#4B9EDB,stroke:#232F3E,color:#fff
        style I fill:#FF9900,stroke:#232F3E,color:#fff
        style J fill:#FF6B35,stroke:#232F3E,color:#fff
    """

def create_load_balancer_types_mermaid():
    """Create mermaid diagram comparing load balancer types"""
    return """
    graph TD
        A[Elastic Load Balancing] --> B[Application Load Balancer]
        A --> C[Network Load Balancer]
        A --> D[Gateway Load Balancer]
        
        B --> E[Layer 7 - HTTP/HTTPS]
        B --> F[Content-based routing]
        B --> G[SSL termination]
        
        C --> H[Layer 4 - TCP/UDP]
        C --> I[Ultra-high performance]
        C --> J[Static IP addresses]
        
        D --> K[Layer 3 - IP packets]
        D --> L[Third-party appliances]
        D --> M[Transparent network gateway]
        
        E --> N[Web Applications]
        H --> O[Gaming, IoT]
        K --> P[Firewalls, IDS/IPS]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C fill:#3FB34F,stroke:#232F3E,color:#fff
        style D fill:#FF6B35,stroke:#232F3E,color:#fff
    """

def create_scaling_policies_mermaid():
    """Create mermaid diagram for scaling policies"""
    return """
    graph TD
        A[Auto Scaling Policies] --> B[Target Tracking]
        A --> C[Step Scaling]
        A --> D[Simple Scaling]
        A --> E[Predictive Scaling]
        
        B --> F[Maintain Target Metric]
        B --> G[CPU Utilization 50%]
        B --> H[Request Count per Target]
        
        C --> I[Multiple Step Adjustments]
        C --> J[Based on Alarm Severity]
        
        D --> K[Single Scaling Action]
        D --> L[Cooldown Period]
        
        E --> M[Machine Learning]
        E --> N[Forecasted Traffic]
        E --> O[Scheduled Scaling]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#3FB34F,stroke:#232F3E,color:#fff
        style C fill:#4B9EDB,stroke:#232F3E,color:#fff
        style D fill:#FF6B35,stroke:#232F3E,color:#fff
        style E fill:#232F3E,stroke:#FF9900,color:#fff
    """

def simulate_auto_scaling():
    """Interactive auto scaling simulator"""
    st.markdown("## 🎮 Auto Scaling Simulator")
    
    # Configuration
    col1, col2, col3 = st.columns(3)
    
    with col1:
        target_cpu = st.slider("Target CPU Utilization (%)", 30, 90, 50)
        scale_out_threshold = st.slider("Scale Out Threshold (%)", 60, 100, 70)
    
    with col2:
        scale_in_threshold = st.slider("Scale In Threshold (%)", 10, 50, 30)
        min_instances = st.slider("Min Instances", 1, 5, 2)
    
    with col3:
        max_instances = st.slider("Max Instances", 5, 20, 10)
        current_instances = st.slider("Current Instances", min_instances, max_instances, 3)
    
    # Traffic pattern simulation
    st.markdown("### 📊 Traffic Pattern Simulation")
    
    # Generate sample data
    hours = np.arange(0, 24)
    base_traffic = 40 + 30 * np.sin((hours - 6) * np.pi / 12)  # Peak at 6 PM
    noise = np.random.normal(0, 5, 24)
    cpu_utilization = np.clip(base_traffic + noise, 0, 100)
    
    # Calculate instance count based on scaling policy
    instance_count = []
    current_count = current_instances
    
    for cpu in cpu_utilization:
        if cpu > scale_out_threshold and current_count < max_instances:
            current_count = min(current_count + 1, max_instances)
        elif cpu < scale_in_threshold and current_count > min_instances:
            current_count = max(current_count - 1, min_instances)
        instance_count.append(current_count)
    
    # Create visualization
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('CPU Utilization and Thresholds', 'Auto Scaling Response'),
        shared_xaxes=True
    )
    
    # CPU utilization plot
    fig.add_trace(go.Scatter(x=hours, y=cpu_utilization, 
                            name='CPU Utilization', 
                            line=dict(color=AWS_COLORS['primary'], width=3)),
                  row=1, col=1)
    
    fig.add_hline(y=scale_out_threshold, line_dash="dash", 
                  line_color=AWS_COLORS['warning'],
                  annotation_text="Scale Out Threshold", row=1, col=1)
    
    fig.add_hline(y=scale_in_threshold, line_dash="dash", 
                  line_color=AWS_COLORS['success'],
                  annotation_text="Scale In Threshold", row=1, col=1)
    
    # Instance count plot
    fig.add_trace(go.Scatter(x=hours, y=instance_count, 
                            name='Instance Count', 
                            line=dict(color=AWS_COLORS['light_blue'], width=3),
                            fill='tonexty'),
                  row=2, col=1)
    
    fig.update_layout(height=600, title_text="Auto Scaling Simulation - 24 Hour Period")
    fig.update_xaxes(title_text="Hour of Day", row=2, col=1)
    fig.update_yaxes(title_text="CPU %", row=1, col=1)
    fig.update_yaxes(title_text="Instance Count", row=2, col=1)
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f"""
        **Max Instances**  
        {max(instance_count)}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f"""
        **Avg Instances**  
        {np.mean(instance_count):.1f}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f"""
        **Scale Events**  
        {len(np.where(np.diff(instance_count) != 0)[0])}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col4:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f"""
        **Cost Savings**  
        {((max_instances * 24 - sum(instance_count)) / (max_instances * 24) * 100):.1f}%
        """)
        st.markdown('</div>', unsafe_allow_html=True)

def amazon_ec2_auto_scaling_tab():
    """Content for Amazon EC2 Auto Scaling tab"""
    st.markdown("# 📈 Amazon EC2 Auto Scaling")
    st.markdown("*Maintain application availability and scale Amazon EC2 capacity up or down automatically*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **Amazon EC2 Auto Scaling** helps you ensure that you have the correct number of Amazon EC2 instances 
    available to handle the load for your application. It automatically scales your EC2 capacity up or down 
    according to conditions you define.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Auto Scaling Architecture
    st.markdown("## 🏗️ Auto Scaling Architecture")
    common.mermaid(create_auto_scaling_architecture_mermaid(), height=400)
    
    # Scaling Policies Overview
    st.markdown("## 📋 Scaling Policies")
    common.mermaid(create_scaling_policies_mermaid(), height=350)
    
    # Interactive Auto Scaling Simulator
    simulate_auto_scaling()
    
    # Scaling Policy Types Comparison
    st.markdown("## 🔍 Scaling Policy Types Comparison")
    
    policy_data = {
        'Policy Type': ['Target Tracking', 'Step Scaling', 'Simple Scaling', 'Predictive Scaling'],
        'Complexity': ['Low', 'Medium', 'Low', 'High'],
        'Response Time': ['Fast', 'Very Fast', 'Medium', 'Proactive'],
        'Use Case': ['General purpose', 'Complex scenarios', 'Basic scaling', 'Predictable patterns'],
        'Cooldown': ['Built-in', 'Configurable', 'Required', 'ML-optimized']
    }
    
    df_policies = pd.DataFrame(policy_data)
    st.dataframe(df_policies, use_container_width=True)
    
    # Benefits of Auto Scaling
    st.markdown("## ✨ Benefits of Auto Scaling")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 💰 Cost Optimization
        - **Pay only for what you use**
        - Automatic scale-in during low demand
        - Eliminate over-provisioning
        - **Savings**: Up to 50-70% on compute costs
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🚀 Performance
        - **Maintain application responsiveness**
        - Handle traffic spikes automatically
        - Prevent downtime from capacity issues
        - **SLA**: Maintain 99.9%+ availability
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔧 Operational Excellence
        - **Reduce manual intervention**
        - Self-healing infrastructure
        - Predictable scaling behavior
        - **Time Saved**: 80% less manual scaling
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Creating Auto Scaling Policy")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
# Create Auto Scaling policy with Target Tracking
import boto3

autoscaling = boto3.client('autoscaling')
cloudwatch = boto3.client('cloudwatch')

# Create Auto Scaling Group (assuming launch template exists)
asg_response = autoscaling.create_auto_scaling_group(
    AutoScalingGroupName='my-web-app-asg',
    LaunchTemplate={
        'LaunchTemplateName': 'my-web-app-template',
        'Version': '$Latest'
    },
    MinSize=2,
    MaxSize=10,
    DesiredCapacity=3,
    TargetGroupARNs=[
        'arn:aws:elasticloadbalancing:us-east-1:123456789:targetgroup/my-targets/1234567890123456'
    ],
    VPCZoneIdentifier='subnet-12345,subnet-67890',
    HealthCheckType='ELB',
    HealthCheckGracePeriod=300,
    Tags=[
        {
            'Key': 'Name',
            'Value': 'WebApp-ASG-Instance',
            'PropagateAtLaunch': True,
            'ResourceId': 'my-web-app-asg',
            'ResourceType': 'auto-scaling-group'
        }
    ]
)

print(f"Auto Scaling Group created: my-web-app-asg")

# Create Target Tracking Scaling Policy
scaling_policy = autoscaling.put_scaling_policy(
    AutoScalingGroupName='my-web-app-asg',
    PolicyName='cpu-target-tracking-policy',
    PolicyType='TargetTrackingScaling',
    TargetTrackingConfiguration={
        'PredefinedMetricSpecification': {
            'PredefinedMetricType': 'ASGAverageCPUUtilization'
        },
        'TargetValue': 50.0,
        'ScaleOutCooldown': 300,
        'ScaleInCooldown': 300
    }
)

print(f"Target Tracking Policy created: {scaling_policy['PolicyARN']}")

# Create Step Scaling Policy for more granular control
step_policy = autoscaling.put_scaling_policy(
    AutoScalingGroupName='my-web-app-asg',
    PolicyName='cpu-step-scaling-policy',
    PolicyType='StepScaling',
    AdjustmentType='ChangeInCapacity',
    StepAdjustments=[
        {
            'MetricIntervalLowerBound': 0,
            'MetricIntervalUpperBound': 20,
            'ScalingAdjustment': 1
        },
        {
            'MetricIntervalLowerBound': 20,
            'ScalingAdjustment': 2
        }
    ]
)

print(f"Step Scaling Policy created: {step_policy['PolicyARN']}")

# Create CloudWatch Alarm to trigger step scaling
cloudwatch.put_metric_alarm(
    AlarmName='high-cpu-alarm',
    ComparisonOperator='GreaterThanThreshold',
    EvaluationPeriods=2,
    MetricName='CPUUtilization',
    Namespace='AWS/EC2',
    Period=300,
    Statistic='Average',
    Threshold=70.0,
    ActionsEnabled=True,
    AlarmActions=[step_policy['PolicyARN']],
    AlarmDescription='Trigger scaling when CPU exceeds 70%',
    Dimensions=[
        {
            'Name': 'AutoScalingGroupName',
            'Value': 'my-web-app-asg'
        }
    ]
)

print("CloudWatch alarm created for high CPU utilization")
    """, language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def auto_scaling_groups_tab():
    """Content for Auto Scaling Groups tab"""
    st.markdown("# 👥 Amazon EC2 Auto Scaling Groups")
    st.markdown("*Collection of EC2 instances treated as a logical grouping for scaling and management*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    An **Auto Scaling Group (ASG)** contains a collection of EC2 instances that are treated as a logical grouping 
    for the purposes of automatic scaling and management. It ensures that your group never goes below or above 
    the minimum or maximum number of instances that you specify.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # ASG Lifecycle
    st.markdown("## 🔄 Auto Scaling Group Lifecycle")
    common.mermaid(create_asg_lifecycle_mermaid(), height=300)
    
    # Interactive ASG Configuration
    st.markdown("## 🛠️ Interactive ASG Configuration")
    
    st.markdown('<div class="scaling-simulator">', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Basic Configuration")
        asg_name = st.text_input("Auto Scaling Group Name:", "my-web-app-asg")
        
        min_size = st.slider("Minimum Size:", 0, 5, 2)
        max_size = st.slider("Maximum Size:", 5, 50, 10)
        desired_capacity = st.slider("Desired Capacity:", min_size, max_size, 3)
        
        availability_zones = st.multiselect("Availability Zones:", [
            "us-east-1a", "us-east-1b", "us-east-1c", "us-east-1d"
        ], default=["us-east-1a", "us-east-1b"])
    
    with col2:
        st.markdown("### Health Check Configuration")
        health_check_type = st.selectbox("Health Check Type:", [
            "EC2 (Instance status)", "ELB (Load balancer health)"
        ])
        
        health_check_grace_period = st.slider("Health Check Grace Period (seconds):", 60, 600, 300)
        
        termination_policies = st.multiselect("Termination Policies:", [
            "Default", "OldestInstance", "NewestInstance", "OldestLaunchConfiguration", "ClosestToNextInstanceHour"
        ], default=["Default"])
        
        instance_protection = st.checkbox("Enable Instance Scale-in Protection")
    
    # Launch Template Configuration
    st.markdown("### Launch Template Configuration")
    col3, col4 = st.columns(2)
    
    with col3:
        launch_template_name = st.text_input("Launch Template Name:", "my-web-app-template")
        instance_type = st.selectbox("Instance Type:", [
            "t3.micro", "t3.small", "t3.medium", "m5.large", "m5.xlarge"
        ])
        ami_id = st.text_input("AMI ID:", "ami-0abcdef1234567890")
    
    with col4:
        key_pair = st.text_input("Key Pair:", "my-key-pair")
        security_groups = st.text_input("Security Groups:", "sg-12345678,sg-87654321")
        user_data = st.text_area("User Data Script:", 
"""#!/bin/bash
yum update -y
yum install -y httpd
systemctl start httpd
systemctl enable httpd""")
    
    if st.button("🚀 Create Auto Scaling Group (Simulation)", use_container_width=True):
        
        # Simulate cost calculation
        hourly_cost = {
            "t3.micro": 0.0104, "t3.small": 0.0208, "t3.medium": 0.0416,
            "m5.large": 0.096, "m5.xlarge": 0.192
        }
        
        estimated_hourly = hourly_cost.get(instance_type, 0.05) * desired_capacity
        estimated_monthly = estimated_hourly * 24 * 30
        
        st.markdown('<div class="success-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ Auto Scaling Group Created Successfully!
        
        **ASG Configuration:**
        - **Name**: {asg_name}
        - **Capacity**: Min: {min_size}, Desired: {desired_capacity}, Max: {max_size}
        - **Availability Zones**: {', '.join(availability_zones)}
        - **Health Check**: {health_check_type} ({health_check_grace_period}s grace period)
        - **Instance Type**: {instance_type}
        
        **Cost Estimation:**
        - **Hourly**: ${estimated_hourly:.3f} ({desired_capacity} instances)
        - **Monthly**: ${estimated_monthly:.2f} (estimated)
        
        🎯 Your ASG will automatically maintain {desired_capacity} healthy instances!
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # ASG Best Practices
    st.markdown("## 💡 Auto Scaling Group Best Practices")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🏗️ Design Best Practices
        - **Use multiple AZs** for high availability
        - **Configure health checks** properly
        - **Set appropriate capacity limits**
        - **Use launch templates** instead of launch configurations
        - **Tag instances** for better management
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚙️ Operational Best Practices  
        - **Monitor scaling activities** in CloudWatch
        - **Test scaling policies** thoroughly
        - **Use predictive scaling** for known patterns
        - **Implement proper cooldown periods**
        - **Plan for graceful shutdown**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Instance Lifecycle Management
    st.markdown("## 🔄 Instance Lifecycle Management")
    
    lifecycle_data = {
        'State': ['Pending', 'InService', 'Terminating', 'Terminated', 'Detached'],
        'Description': [
            'Instance is launching and not yet ready',
            'Instance is healthy and receiving traffic',
            'Instance is being shut down gracefully',
            'Instance has been terminated',
            'Instance removed from ASG but still running'
        ],
        'Health Check': ['Not Started', 'Active', 'Suspended', 'N/A', 'N/A'],
        'Traffic': ['No', 'Yes', 'Draining', 'No', 'External']
    }
    
    df_lifecycle = pd.DataFrame(lifecycle_data)
    st.dataframe(df_lifecycle, use_container_width=True)
    
    # Instance Refresh Feature
    st.markdown("## 🔄 Instance Refresh")
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown("""
    ### 🆕 Instance Refresh Feature
    
    **Instance Refresh** allows you to update instances in your Auto Scaling group by gradually replacing them 
    with new instances launched from an updated launch template.
    
    **Key Benefits:**
    - ✅ **Rolling Updates**: Replace instances gradually to maintain availability
    - ✅ **Automated Process**: No manual intervention required
    - ✅ **Health Monitoring**: Ensures new instances are healthy before continuing
    - ✅ **Rollback Capability**: Cancel refresh if issues are detected
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Advanced ASG Configuration")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
# Advanced Auto Scaling Group configuration with mixed instance types
import boto3

ec2 = boto3.client('ec2')
autoscaling = boto3.client('autoscaling')

# Create Launch Template with multiple instance types
launch_template_response = ec2.create_launch_template(
    LaunchTemplateName='my-mixed-instance-template',
    LaunchTemplateData={
        'ImageId': 'ami-0abcdef1234567890',
        'InstanceType': 't3.medium',  # Default instance type
        'KeyName': 'my-key-pair',
        'SecurityGroupIds': ['sg-12345678'],
        'IamInstanceProfile': {
            'Name': 'EC2-Instance-Profile'
        },
        'UserData': '''IyEvYmluL2Jhc2gKZWNobyAiSGVsbG8gZnJvbSBBU0cgaW5zdGFuY2UiCg==''',  # Base64 encoded
        'TagSpecifications': [
            {
                'ResourceType': 'instance',
                'Tags': [
                    {'Key': 'Name', 'Value': 'ASG-WebServer'},
                    {'Key': 'Environment', 'Value': 'Production'}
                ]
            }
        ]
    }
)

print(f"Launch Template created: {launch_template_response['LaunchTemplate']['LaunchTemplateId']}")

# Create Auto Scaling Group with mixed instance types
asg_response = autoscaling.create_auto_scaling_group(
    AutoScalingGroupName='mixed-instance-asg',
    MixedInstancesPolicy={
        'LaunchTemplate': {
            'LaunchTemplateSpecification': {
                'LaunchTemplateName': 'my-mixed-instance-template',
                'Version': '$Latest'
            },
            'Overrides': [
                {'InstanceType': 't3.medium', 'WeightedCapacity': '1'},
                {'InstanceType': 't3.large', 'WeightedCapacity': '2'},
                {'InstanceType': 'm5.large', 'WeightedCapacity': '2'}
            ]
        },
        'InstancesDistribution': {
            'OnDemandAllocationStrategy': 'prioritized',
            'OnDemandBaseCapacity': 1,
            'OnDemandPercentageAboveBaseCapacity': 50,
            'SpotAllocationStrategy': 'capacity-optimized'
        }
    },
    MinSize=2,
    MaxSize=20,
    DesiredCapacity=4,
    VPCZoneIdentifier='subnet-12345,subnet-67890,subnet-abcdef',
    HealthCheckType='ELB',
    HealthCheckGracePeriod=300,
    DefaultCooldown=300,
    Tags=[
        {
            'Key': 'Name',
            'Value': 'MixedInstance-ASG',
            'PropagateAtLaunch': True,
            'ResourceId': 'mixed-instance-asg',
            'ResourceType': 'auto-scaling-group'
        }
    ]
)

print("Mixed instance Auto Scaling Group created with Spot and On-Demand instances")

# Configure Instance Refresh
refresh_response = autoscaling.start_instance_refresh(
    AutoScalingGroupName='mixed-instance-asg',
    Strategy='Rolling',
    Preferences={
        'InstanceWarmup': 300,
        'MinHealthyPercentage': 90,
        'CheckpointPercentages': [20, 50, 100],
        'CheckpointDelay': 3600
    }
)

print(f"Instance refresh started: {refresh_response['InstanceRefreshId']}")
    """, language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def elastic_load_balancing_tab():
    """Content for Elastic Load Balancing tab"""
    st.markdown("# ⚖️ Elastic Load Balancing")
    st.markdown("*Automatically distribute incoming application traffic across multiple targets*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **Elastic Load Balancing (ELB)** automatically distributes incoming application traffic across multiple targets, 
    such as Amazon EC2 instances, containers, and IP addresses. It can handle varying load of your application 
    traffic in a single Availability Zone or across multiple Availability zones.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Load Balancer Types
    st.markdown("## 🔍 Load Balancer Types")
    common.mermaid(create_load_balancer_types_mermaid(), height=400)
    
    # Interactive Load Balancer Comparison
    st.markdown("## 📊 Load Balancer Types Comparison")
    
    comparison_data = {
        'Feature': [
            'OSI Layer', 'Protocols', 'Use Cases', 'SSL Termination', 
            'WebSocket Support', 'Static IP', 'Preserve Source IP', 'Latency'
        ],
        'Application Load Balancer': [
            'Layer 7', 'HTTP, HTTPS, gRPC', 'Web applications, Microservices', 
            'Yes', 'Yes', 'No (DNS name)', 'Optional', 'Low'
        ],
        'Network Load Balancer': [
            'Layer 4', 'TCP, UDP, TLS', 'Gaming, IoT, Ultra-high performance', 
            'Yes', 'Yes', 'Yes', 'Yes', 'Ultra-low'
        ],
        'Gateway Load Balancer': [
            'Layer 3', 'All IP traffic', 'Third-party appliances', 
            'No', 'N/A', 'No', 'Yes', 'Minimal'
        ]
    }
    
    df_comparison = pd.DataFrame(comparison_data)
    st.dataframe(df_comparison, use_container_width=True)
    
    # Interactive Load Balancer Configuration
    st.markdown("## 🔧 Interactive Load Balancer Configuration")
    
    lb_type = st.selectbox("Choose Load Balancer Type:", [
        "Application Load Balancer", "Network Load Balancer", "Gateway Load Balancer"
    ])
    
    if lb_type == "Application Load Balancer":
        configure_application_load_balancer()
    elif lb_type == "Network Load Balancer":
        configure_network_load_balancer()
    else:
        configure_gateway_load_balancer()
    
    # Load Balancing Algorithms
    st.markdown("## 🎯 Load Balancing Algorithms")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚖️ Round Robin
        - **Default for ALB/NLB**
        - Distributes requests evenly
        - Good for homogeneous targets
        - **Best for**: Equal instance capacity
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🎲 Least Outstanding Requests
        - **ALB intelligent routing**
        - Routes to target with fewest active requests
        - Adapts to varying response times
        - **Best for**: Mixed workloads
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🏃‍♂️ Flow Hash (NLB)
        - **Based on protocol, source/destination**
        - Ensures same client goes to same target
        - Good for stateful connections
        - **Best for**: Gaming, real-time apps
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔒 Weighted Target Groups
        - **Manual traffic distribution**
        - Percentage-based routing
        - Perfect for blue/green deployments
        - **Best for**: A/B testing
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Health Checks Deep Dive
    st.markdown("## 🏥 Health Check Configuration")
    
    health_check_demo()
    
    # Code Example
    st.markdown("## 💻 Code Example: Application Load Balancer Setup")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
# Complete Application Load Balancer setup with Auto Scaling Group
import boto3

elbv2 = boto3.client('elbv2')
autoscaling = boto3.client('autoscaling')
ec2 = boto3.client('ec2')

# Create Application Load Balancer
alb_response = elbv2.create_load_balancer(
    Name='my-web-app-alb',
    Subnets=['subnet-12345', 'subnet-67890'],  # Must be in different AZs
    SecurityGroups=['sg-alb-security-group'],
    Scheme='internet-facing',
    Tags=[
        {'Key': 'Name', 'Value': 'WebApp-ALB'},
        {'Key': 'Environment', 'Value': 'Production'}
    ],
    Type='application',
    IpAddressType='ipv4'
)

alb_arn = alb_response['LoadBalancers'][0]['LoadBalancerArn']
alb_dns = alb_response['LoadBalancers'][0]['DNSName']
print(f"ALB created: {alb_dns}")

# Create Target Group
target_group_response = elbv2.create_target_group(
    Name='my-web-app-targets',
    Protocol='HTTP',
    Port=80,
    VpcId='vpc-12345678',
    TargetType='instance',
    HealthCheckProtocol='HTTP',
    HealthCheckPath='/health',
    HealthCheckIntervalSeconds=30,
    HealthCheckTimeoutSeconds=5,
    HealthyThresholdCount=2,
    UnhealthyThresholdCount=3,
    Matcher={'HttpCode': '200'},
    Tags=[
        {'Key': 'Name', 'Value': 'WebApp-Targets'}
    ]
)

target_group_arn = target_group_response['TargetGroups'][0]['TargetGroupArn']
print(f"Target Group created: {target_group_arn}")

# Create Listener with SSL certificate
listener_response = elbv2.create_listener(
    LoadBalancerArn=alb_arn,
    Protocol='HTTPS',
    Port=443,
    SslPolicy='ELBSecurityPolicy-TLS-1-2-2019-07',
    Certificates=[
        {'CertificateArn': 'arn:aws:acm:us-east-1:123456789:certificate/12345678-1234-1234-1234-123456789012'}
    ],
    DefaultActions=[
        {
            'Type': 'forward',
            'TargetGroupArn': target_group_arn
        }
    ]
)

print(f"HTTPS Listener created: {listener_response['Listeners'][0]['ListenerArn']}")

# Create HTTP to HTTPS redirect listener
redirect_listener = elbv2.create_listener(
    LoadBalancerArn=alb_arn,
    Protocol='HTTP',
    Port=80,
    DefaultActions=[
        {
            'Type': 'redirect',
            'RedirectConfig': {
                'Protocol': 'HTTPS',
                'Port': '443',
                'StatusCode': 'HTTP_301'
            }
        }
    ]
)

# Create Auto Scaling Group and attach to Target Group
autoscaling.create_auto_scaling_group(
    AutoScalingGroupName='my-web-app-asg',
    LaunchTemplate={
        'LaunchTemplateName': 'my-web-app-template',
        'Version': '$Latest'
    },
    MinSize=2,
    MaxSize=10,
    DesiredCapacity=3,
    TargetGroupARNs=[target_group_arn],
    VPCZoneIdentifier='subnet-12345,subnet-67890',
    HealthCheckType='ELB',
    HealthCheckGracePeriod=300
)

print("Auto Scaling Group created and attached to ALB Target Group")

# Configure advanced routing rules
advanced_listener_rule = elbv2.create_rule(
    ListenerArn=listener_response['Listeners'][0]['ListenerArn'],
    Priority=100,
    Conditions=[
        {
            'Field': 'path-pattern',
            'Values': ['/api/*']
        }
    ],
    Actions=[
        {
            'Type': 'forward',
            'TargetGroupArn': target_group_arn
        }
    ]
)

print("Advanced routing rule created for /api/* paths")
    """, language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def configure_application_load_balancer():
    """Configure Application Load Balancer"""
    st.markdown('<div class="scaling-simulator">', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Basic Configuration")
        alb_name = st.text_input("Load Balancer Name:", "my-web-app-alb")
        scheme = st.selectbox("Scheme:", ["Internet-facing", "Internal"])
        
        subnets = st.multiselect("Subnets:", [
            "subnet-12345 (us-east-1a)", "subnet-67890 (us-east-1b)", 
            "subnet-abcde (us-east-1c)"
        ], default=["subnet-12345 (us-east-1a)", "subnet-67890 (us-east-1b)"])
    
    with col2:
        st.markdown("### Security & SSL")
        security_groups = st.text_input("Security Groups:", "sg-alb-web-traffic")
        ssl_certificate = st.selectbox("SSL Certificate:", [
            "None", "AWS Certificate Manager", "IAM Certificate"
        ])
        
        if ssl_certificate != "None":
            ssl_policy = st.selectbox("SSL Policy:", [
                "ELBSecurityPolicy-TLS-1-2-2019-07",
                "ELBSecurityPolicy-FS-1-2-2019-08",
                "ELBSecurityPolicy-TLS-1-1-2017-01"
            ])
    
    # Target Group Configuration
    st.markdown("### Target Group Configuration")
    col3, col4 = st.columns(2)
    
    with col3:
        target_group_name = st.text_input("Target Group Name:", "my-web-app-targets")
        protocol = st.selectbox("Protocol:", ["HTTP", "HTTPS"])
        port = st.number_input("Port:", 1, 65535, 80)
    
    with col4:
        health_check_path = st.text_input("Health Check Path:", "/health")
        health_check_interval = st.slider("Health Check Interval (seconds):", 5, 300, 30)
        healthy_threshold = st.slider("Healthy Threshold:", 2, 10, 2, key='healthy_threshold_2')
    
    if st.button("🚀 Create Application Load Balancer (Simulation)", use_container_width=True):
        
        st.markdown('<div class="success-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ Application Load Balancer Created Successfully!
        
        **Load Balancer Details:**
        - **Name**: {alb_name}
        - **Scheme**: {scheme}
        - **Subnets**: {len(subnets)} availability zones
        - **SSL**: {ssl_certificate}
        - **DNS Name**: {alb_name}-1234567890.{('us-east-1' if 'us-east-1' in str(subnets) else 'us-west-2')}.elb.amazonaws.com  
        
        **Target Group:**
        - **Name**: {target_group_name}
        - **Protocol**: {protocol}:{port}
        - **Health Check**: {health_check_path} every {health_check_interval}s
        
        🎯 Your ALB can handle Layer 7 routing with SSL termination!
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)

def configure_network_load_balancer():
    """Configure Network Load Balancer"""
    st.markdown('<div class="scaling-simulator">', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Basic Configuration")
        nlb_name = st.text_input("Load Balancer Name:", "my-app-nlb")
        scheme = st.selectbox("Scheme:", ["Internet-facing", "Internal"])
        
        enable_static_ip = st.checkbox("Enable Static IP addresses")
        cross_zone_balancing = st.checkbox("Enable Cross-Zone Load Balancing", value=True)
    
    with col2:
        st.markdown("### Performance Settings")
        protocol = st.selectbox("Protocol:", ["TCP", "UDP", "TCP_UDP", "TLS"])
        port = st.number_input("Port:", 1, 65535, 80)
        
        preserve_source_ip = st.checkbox("Preserve Source IP", value=True)
        proxy_protocol = st.checkbox("Enable Proxy Protocol v2")
    
    if st.button("🚀 Create Network Load Balancer (Simulation)", use_container_width=True):
        
        st.markdown('<div class="success-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ Network Load Balancer Created Successfully!
        
        **Load Balancer Details:**
        - **Name**: {nlb_name}
        - **Scheme**: {scheme}
        - **Protocol**: {protocol}:{port}
        - **Static IP**: {'Yes' if enable_static_ip else 'No'}
        - **Cross-Zone**: {'Enabled' if cross_zone_balancing else 'Disabled'}
        
        **Performance Features:**
        - **Ultra-low latency**: < 100 microseconds
        - **High throughput**: Millions of requests per second
        - **Source IP preservation**: {'Yes' if preserve_source_ip else 'No'}
        
        ⚡ Your NLB provides ultra-high performance at Layer 4!
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)

def configure_gateway_load_balancer():
    """Configure Gateway Load Balancer"""
    st.markdown('<div class="scaling-simulator">', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Basic Configuration")
        gwlb_name = st.text_input("Gateway Load Balancer Name:", "my-security-gwlb")
        
        appliance_type = st.selectbox("Third-party Appliance:", [
            "Firewall", "Intrusion Detection System", "Deep Packet Inspection", "Custom Security Appliance"
        ])
    
    with col2:
        st.markdown("### Network Configuration")
        vpc_endpoint = st.checkbox("Create VPC Endpoint Service")
        geneve_port = st.number_input("GENEVE Port:", 1, 65535, 6081)
        
        flow_stickiness = st.selectbox("Flow Stickiness:", [
            "5-tuple (default)", "3-tuple", "2-tuple"
        ])
    
    if st.button("🚀 Create Gateway Load Balancer (Simulation)", use_container_width=True):
        
        st.markdown('<div class="success-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ Gateway Load Balancer Created Successfully!
        
        **Load Balancer Details:**
        - **Name**: {gwlb_name}
        - **Appliance Type**: {appliance_type}
        - **GENEVE Port**: {geneve_port}
        - **Flow Stickiness**: {flow_stickiness}
        - **VPC Endpoint**: {'Yes' if vpc_endpoint else 'No'}
        
        **Use Cases:**
        - **Transparent network gateway**
        - **Third-party security appliances**
        - **Centralized traffic inspection**
        
        🛡️ Your GWLB enables transparent security inspection at scale!
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)

def health_check_demo():
    """Interactive health check configuration demo"""
    st.markdown("### 🏥 Health Check Configuration Demo")
    
    col1, col2 = st.columns(2)
    
    with col1:
        protocol = st.selectbox("Health Check Protocol:", ["HTTP", "HTTPS", "TCP"])
        if protocol in ["HTTP", "HTTPS"]:
            path = st.text_input("Health Check Path:", "/health", key='health_path')
            success_codes = st.text_input("Success Codes:", "200")
        
        port = st.selectbox("Health Check Port:", ["Traffic Port", "Override Port"])
        if port == "Override Port":
            custom_port = st.number_input("Custom Port:", 1, 65535, 8080)
    
    with col2:
        interval = st.slider("Interval (seconds):", 5, 300, 30)
        timeout = st.slider("Timeout (seconds):", 2, 120, 5)
        healthy_threshold = st.slider("Healthy Threshold:", 2, 10, 2)
        unhealthy_threshold = st.slider("Unhealthy Threshold:", 2, 10, 3)
    
    # Health Check Timeline Simulation
    st.markdown("#### Health Check Timeline Simulation")
    
    # Generate sample health check data
    timestamps = pd.date_range(start='2025-01-01 00:00:00', periods=20, freq=f'{interval}S')
    
    # Simulate health check responses
    np.random.seed(42)
    responses = []
    current_health = "Healthy"
    
    for i in range(20):
        # Simulate occasional failures
        if np.random.random() < 0.15:  # 15% chance of failure
            response_code = np.random.choice([500, 503, 404])
            current_health = "Unhealthy"
        else:
            response_code = 200
            current_health = "Healthy"
        
        responses.append({
            'timestamp': timestamps[i],
            'response_code': response_code,
            'health_status': current_health,
            'response_time': np.random.uniform(10, 100)
        })
    
    df_health = pd.DataFrame(responses)
    
    # Create health check visualization
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Health Check Response Codes', 'Response Time'),
        shared_xaxes=True
    )
    
    # Response codes
    colors = ['red' if code != 200 else 'green' for code in df_health['response_code']]
    fig.add_trace(go.Scatter(
        x=df_health['timestamp'], 
        y=df_health['response_code'],
        mode='markers+lines',
        marker=dict(color=colors, size=8),
        name='Response Code'
    ), row=1, col=1)
    
    # Response time
    fig.add_trace(go.Scatter(
        x=df_health['timestamp'], 
        y=df_health['response_time'],
        mode='lines+markers',
        marker_color=AWS_COLORS['light_blue'],
        name='Response Time (ms)'
    ), row=2, col=1)
    
    fig.update_layout(height=400, title_text="Health Check Monitoring")
    fig.update_xaxes(title_text="Time", row=2, col=1)
    fig.update_yaxes(title_text="HTTP Status", row=1, col=1)
    fig.update_yaxes(title_text="Response Time (ms)", row=2, col=1)
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Health check summary
    healthy_count = len(df_health[df_health['response_code'] == 200])
    health_percentage = (healthy_count / len(df_health)) * 100
    
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.markdown(f"""
    **Health Check Summary**  
    Success Rate: {health_percentage:.1f}%  
    Avg Response Time: {df_health['response_time'].mean():.1f}ms  
    Failed Checks: {len(df_health) - healthy_count}
    """)
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
    # ⚖️ AWS Scaling and Load Balancing
    ### Master AWS auto scaling and load balancing for resilient, cost-optimized applications
    """)
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs([
        "📈 Amazon EC2 Auto Scaling", 
        "👥 Auto Scaling Groups", 
        "⚖️ Elastic Load Balancing"
    ])
    
    with tab1:
        amazon_ec2_auto_scaling_tab()
    
    with tab2:
        auto_scaling_groups_tab()
    
    with tab3:
        elastic_load_balancing_tab()
    
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
